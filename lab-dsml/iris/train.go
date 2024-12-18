package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	gc "cs426.yale.edu/lab-dsml/gpu_coordinator/server_lib"
	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
	data "cs426.yale.edu/lab-dsml/iris/data"
	utils "cs426.yale.edu/lab-dsml/utils"
	"github.com/pkg/errors"
	"gonum.org/v1/gonum/mat"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// returns X, Y, W, Grads, error
func initIrisData(numDevices uint32) ([][]byte, [][]byte, []byte, [][]byte, *mat.Dense, *mat.Dense, error) {
	rng := rand.New(rand.NewSource(42))

	trainFeatures, trainTargets, testFeatures, testTargets, err := data.LoadIrisDataset("data/iris.csv", 0.2)
	if err != nil {
		return [][]byte{}, [][]byte{}, []byte{}, [][]byte{}, nil, nil, errors.Errorf("failed to load iris dataset: %v", err)
	}
	splitFeatures := utils.SplitMatrix(trainFeatures, int(numDevices))
	splitTargets := utils.SplitMatrix(trainTargets, int(numDevices))

	var serializedFeatures [][]byte
	for _, featureChunk := range splitFeatures {
		bytes, err := utils.SerializeMatrix(featureChunk)
		if err != nil {
			return [][]byte{}, [][]byte{}, []byte{}, [][]byte{}, nil, nil, errors.Errorf("failed to serialize features: %v", err)
		}
		serializedFeatures = append(serializedFeatures, bytes)
	}

	var serializedTargets [][]byte
	for _, targetChunk := range splitTargets {
		bytes, err := utils.SerializeMatrix(targetChunk)
		if err != nil {
			return [][]byte{}, [][]byte{}, []byte{}, [][]byte{}, nil, nil, errors.Errorf("failed to serialize targets: %v", err)
		}
		serializedTargets = append(serializedTargets, bytes)
	}

	// dimensions are hardcoded for iris dataset
	// nFeatures is 4 + 1 = 5 to account for a column of 1s for the bias
	nFeatures, nOutputs := 5, 3
	data := make([]float64, nFeatures*nOutputs)
	for i := range data {
		data[i] = rng.Float64() // Random value in [0.0, 1.0)
	}
	weightMatrix := mat.NewDense(nFeatures, nOutputs, data)
	serializedWeight, err := utils.SerializeMatrix(weightMatrix)
	if err != nil {
		return [][]byte{}, [][]byte{}, []byte{}, [][]byte{}, nil, nil, errors.Errorf("failed to serialize targets: %v", err)
	}

	// chunked gradients, initialized to 0
	gradients := mat.NewDense(nFeatures, nOutputs, nil)
	gradientChunks := utils.SplitMatrix(gradients, int(numDevices))
	var serializedGradients [][]byte
	for _, gradientChunk := range gradientChunks {
		bytes, err := utils.SerializeMatrix(gradientChunk)
		if err != nil {
			return [][]byte{}, [][]byte{}, []byte{}, [][]byte{}, nil, nil, errors.Errorf("failed to serialize gradients: %v", err)
		}
		serializedGradients = append(serializedGradients, bytes)
	}

	return serializedFeatures, serializedTargets, serializedWeight, serializedGradients, testFeatures, testTargets, nil
}

func main() {
	// hard coded values based on gpu_config.json
	coordinatorAddress := "[::1]:8080"
	var numDevices uint32 = 4

	// training hyperparameters
	// divide lr by numDevices because the gradients are summed, not averaged
	epochs := 100
	learningRate := 5e-2 / float64(numDevices)
	lrDecay := 0.999

	deviceMemMap := make(map[uint64]*gc.Memory)

	// Load the Iris dataset
	serializedFeatures, serializedTargets, serializedWeight, serializedGradients, testFeatures, testTargets, err := initIrisData(numDevices)
	if err != nil {
		log.Fatalf("failed to load iris dataset: %v", err)
	}

	// start coordinator client, init comm, and get metadata
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(coordinatorAddress, opts...)
	if err != nil {
		log.Fatalf("failed to connect to GPU Coordinator Server: %v", err)
	}
	ctx := context.Background()
	client := pb.NewGPUCoordinatorClient(conn)
	initResponse, err := client.CommInit(ctx, &pb.CommInitRequest{NumDevices: numDevices})
	if err != nil {
		log.Fatalf("failed to init connection: %v", err)
	}
	if !initResponse.Success {
		log.Fatalf("failed to init connection")
	}
	commId := initResponse.CommId
	log.Printf("Using commId: %v", commId)
	metaData := initResponse.Devices
	if metaData == nil {
		log.Fatalf("failed to retrieve metadata for devices")
	}

	// process the chunked gradients and get their sizes
	var gradientSizes []uint64
	var flattenedGradients []byte
	for _, gradient := range serializedGradients {
		gradientSizes = append(gradientSizes, uint64(len(gradient)))
		flattenedGradients = append(flattenedGradients, gradient...)
	}

	// process metadata and initialize memory map with data, addresses, and sizes
	var deviceIds []uint64
	for i, metaDatum := range metaData {
		deviceId := metaDatum.DeviceId.Value
		minAddr := metaDatum.MinMemAddr.Value
		maxAddr := metaDatum.MaxMemAddr.Value
		deviceIds = append(deviceIds, deviceId)

		// needs to store X, Y, W, and gradients
		inputSize := uint64(len(serializedFeatures[i]))
		weightSize := uint64(len(serializedWeight))
		outputSize := uint64(len(serializedTargets[i]))
		gradientSize := uint64(len(flattenedGradients))
		neededMemory := inputSize + weightSize + outputSize + gradientSize
		if (maxAddr - minAddr) < neededMemory {
			log.Fatalf("insufficient memory on GPU %v", deviceId)
		}

		// compute the memory addresses of each of the matrices
		// this is necessary before concatenating them into a single []byte
		inputAddr := minAddr
		weightAddr := minAddr + inputSize
		outputAddr := weightAddr + weightSize
		var gradientAddrs []uint64
		currGradientAddr := outputAddr + outputSize
		for _, gradientSize := range gradientSizes {
			gradientAddrs = append(gradientAddrs, currGradientAddr)
			currGradientAddr += gradientSize
		}

		data := append(serializedFeatures[i], serializedWeight...)
		data = append(data, serializedTargets[i]...)
		data = append(data, flattenedGradients...)

		deviceMemMap[deviceId] = &gc.Memory{
			Data:          &data,
			InputAddr:     inputAddr,
			InputSize:     inputSize,
			WeightAddr:    weightAddr,
			WeightSize:    weightSize,
			OutputAddr:    outputAddr,
			OutputSize:    outputSize,
			GradientAddrs: gradientAddrs,
			GradientSizes: gradientSizes,
		}
	}

	// Memcpy data over to all the GPUs
	for _, deviceId := range deviceIds {
		memory := deviceMemMap[deviceId]
		hostToDeviceResponse, err := client.Memcpy(ctx, &pb.MemcpyRequest{
			Either: &pb.MemcpyRequest_HostToDevice{
				HostToDevice: &pb.MemcpyHostToDeviceRequest{
					HostSrcData: *memory.Data,
					DstDeviceId: &pb.DeviceId{Value: deviceId},
					DstMemAddr:  &pb.MemAddr{Value: memory.InputAddr},
				},
			},
		})
		if err != nil {
			log.Fatalf("failed to memcpy to device %v", deviceId)
		}
		switch r := hostToDeviceResponse.Either.(type) {
		case *pb.MemcpyResponse_HostToDevice:
			if !r.HostToDevice.Success {
				log.Fatalf("failed to memcpy to device %v", deviceId)
			}
		default:
			log.Fatalf("received invalid response format when memcpy to device %v", deviceId)
		}
	}

	// compute the flows of communication from which memory address to which memory address during AllReduceRing
	reduceDeviceMemAddrs, gatherDeviceMemAddrs := gc.ComputeDeviceMemAddrsForAllReduceRing(deviceIds, deviceMemMap, numDevices)

	// begin training
	for epoch := range epochs {
		// forward pass + compute local gradients
		forwardRequests := make(map[uint64]*pb.ForwardRequest)
		for deviceId, memory := range deviceMemMap {
			var gradientAddresses []*pb.MemAddr
			for _, gradientAddr := range memory.GradientAddrs {
				gradientAddresses = append(gradientAddresses, &pb.MemAddr{Value: gradientAddr})
			}
			forwardRequests[deviceId] = &pb.ForwardRequest{
				InputAddress:      &pb.MemAddr{Value: memory.InputAddr},
				InputSize:         memory.InputSize,
				WeightAddress:     &pb.MemAddr{Value: memory.WeightAddr},
				WeightSize:        memory.WeightSize,
				OutputAddress:     &pb.MemAddr{Value: memory.OutputAddr},
				OutputSize:        memory.OutputSize,
				GradientAddresses: gradientAddresses,
				ForwardOp:         pb.ForwardOp_LINEAR_CROSS_ENTROPY,
			}
		}
		forwardResponse, err := client.ForwardBroadcast(
			ctx,
			&pb.ForwardBroadcastRequest{
				CommId:          commId,
				ForwardRequests: forwardRequests,
			},
		)
		if err != nil {
			log.Fatalf("couldn't do forward pass broadcast: %v", err)
		}
		if !forwardResponse.Success {
			log.Fatalf("couldn't do forward pass broadcast")
		}

		groupStartResponse, err := client.GroupStart(ctx, &pb.GroupStartRequest{CommId: commId})
		if err != nil {
			log.Fatalf("failed group start on comm %v: %v", commId, err)
		}
		if !groupStartResponse.Success {
			log.Fatalf("failed group start on comm %v", commId)
		}

		// AllReduceRing to sum all gradients
		allReduceResponse, err := client.AllReduceRing(ctx, &pb.AllReduceRingRequest{
			CommId:               commId,
			Op:                   pb.ReduceOp_SUM,
			ReduceDeviceMemAddrs: reduceDeviceMemAddrs,
			GatherDeviceMemAddrs: gatherDeviceMemAddrs,
		})
		if err != nil {
			log.Fatalf("failed all reduce ring on comm %v: %v", commId, err)
		}
		if !allReduceResponse.Success {
			log.Fatalf("failed all reduce ring on comm %v", commId)
		}

		groupEndResponse, err := client.GroupEnd(ctx, &pb.GroupEndRequest{CommId: commId})
		if err != nil {
			log.Fatalf("failed group end on comm %v: %v", commId, err)
		}
		if !groupEndResponse.Success {
			log.Fatalf("failed group end on comm %v", commId)
		}

		// Wait for AllReduceRing to finish
		for {
			commStatusResponse, err := client.GetCommStatus(ctx, &pb.GetCommStatusRequest{CommId: commId})
			if err != nil {
				log.Fatalf("failed to get comm status for comm %v", commId)
			}
			if commStatusResponse.Status == pb.Status_FAILED {
				log.Fatalf("job failed on comm %v", commId)
			}
			if commStatusResponse.Status == pb.Status_SUCCESS {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		// Backward pass to update weights
		backwardsRequests := make(map[uint64]*pb.BackwardRequest)
		for deviceId, memory := range deviceMemMap {
			var gradientAddresses []*pb.MemAddr
			for _, gradientAddr := range memory.GradientAddrs {
				gradientAddresses = append(gradientAddresses, &pb.MemAddr{Value: gradientAddr})
			}
			backwardsRequests[deviceId] = &pb.BackwardRequest{
				WeightAddress:     &pb.MemAddr{Value: memory.WeightAddr},
				WeightSize:        memory.WeightSize,
				GradientAddresses: gradientAddresses,
				GradientSizes:     memory.GradientSizes,
				LearningRate:      learningRate,
			}
		}
		client.BackwardBroadcast(
			ctx,
			&pb.BackwardBroadcastRequest{
				CommId:           commId,
				BackwardRequests: backwardsRequests,
			},
		)

		// Get weight from any GPU
		deviceId0 := deviceIds[0]
		memory0 := deviceMemMap[deviceId0]
		deviceToHostResponse, err := client.Memcpy(ctx, &pb.MemcpyRequest{
			Either: &pb.MemcpyRequest_DeviceToHost{
				DeviceToHost: &pb.MemcpyDeviceToHostRequest{
					SrcDeviceId: &pb.DeviceId{Value: deviceId0},
					SrcMemAddr:  &pb.MemAddr{Value: memory0.WeightAddr},
					NumBytes:    uint64(memory0.WeightSize),
				},
			},
		})
		if err != nil {
			log.Fatalf("failed to get data from device %v", deviceId0)
		}

		var weightBuffer []byte
		switch r := deviceToHostResponse.Either.(type) {
		case *pb.MemcpyResponse_DeviceToHost:
			weightBuffer = r.DeviceToHost.DstData
		default:
			log.Fatalf("invalid response format when memcpy from device %v", deviceId0)
		}
		W, err := utils.DeserializeMatrix(weightBuffer)
		if err != nil {
			log.Fatalf("couldn't deserialize weight matrix: %v", err)
		}

		// compute test loss and accuracy
		nSamples := testFeatures.RawMatrix().Rows
		nOutput := testTargets.RawMatrix().Cols
		predictions := utils.LinearSoftmax(testFeatures, W, nSamples, nOutput)
		testLoss := utils.CrossEntropy(predictions, testTargets, nSamples, nOutput)
		accuracy := utils.ComputeAccuracy(predictions, testTargets)
		log.Printf("epoch %v: training loss: %v | test loss: %v | test accuracy: %v", epoch, forwardResponse.Loss, testLoss, accuracy)

		// exponential learning rate decay
		learningRate *= lrDecay
	}
}
