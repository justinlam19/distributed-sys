package main

import (
	"context"
	"log"
	"math/rand"

	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
	data "cs426.yale.edu/lab-dsml/iris/data"
	utils "cs426.yale.edu/lab-dsml/utils"
	"github.com/pkg/errors"
	"gonum.org/v1/gonum/mat"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Memory struct {
	data          *[]byte
	inputAddr     uint64
	inputSize     uint64
	weightAddr    uint64
	weightSize    uint64
	outputAddr    uint64
	outputSize    uint64
	gradientAddrs []uint64
	gradientSizes []uint64
}

// returns X, Y, W, Grads, error
func initIrisData(numDevices uint32) ([][]byte, [][]byte, []byte, [][]byte, *mat.Dense, *mat.Dense, error) {
	rng := rand.New(rand.NewSource(42))

	trainFeatures, trainTargets, testFeatures, testTargets, err := data.LoadIrisDataset("data/iris.csv", 0.2, rng)
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

// On AllReduceRing, need to make n * (n - 1) rounds twice (once for reduce then once for gather)
// e.g. for 4 devices, with the notation being (rank[rcvId, sendId] ... => rank[id0 id1 id2 id3]):
// REDUCE
// phase1: 0[3,0] 1[0,1] 2[1,2] 3[2,3] => 0[0 1 2 33] 1[00 1 2 3] 2[0 11 2 3] 3[0 1 22 3]
// phase2: 0[2,3] 1[3,0] 2[0,1] 3[1,2] => 0[0 1 222 33] 1[00 1 2 333] 2[000 11 2 3] 3[0 111 22 3]
// phase3: 0[1,2] 1[2,3] 2[3,0] 3[0,1] => 0[0 1111 222 33] 1[00 1 2222 333] 2[000 11 2 3333] 3[0000 111 22 3]
// GATHER
// phase1: 0[0,1] 1[1,2] 2[2,3] 3[3,0] => 0[0000 1111 222 33] 1[00 1111 2222 333] 2[000 11 2222 3333] 3[0000 111 22 3333]
// phase2: 0[3,0] 1[0,1] 2[1,2] 3[2,3] => 0[0000 1111 222 3333] 1[0000 1111 2222 333] 2[000 1111 2222 3333] 3[0000 111 2222 3333]
// phase3: 0[2,3] 1[3,0] 2[0,1] 3[1,2] => 0[0000 1111 2222 3333] 1[0000 1111 2222 3333] 2[0000 1111 2222 3333] 3[0000 1111 2222 3333]
func computeDeviceMemAddrs(deviceIds []uint64, deviceMemMap map[uint64]*Memory, numDevices uint32) ([]*pb.DeviceMemAddr, []*pb.DeviceMemAddr) {
	var reduceDeviceMemAddrs []*pb.DeviceMemAddr
	var gatherDeviceMemAddrs []*pb.DeviceMemAddr
	for offset := range numDevices - 1 {
		// id of gradient chunk to send
		i := (numDevices - offset) % numDevices
		for _, srcId := range deviceIds {
			dstId := (srcId + 1) % uint64(len(deviceIds))
			srcMemory := deviceMemMap[srcId]
			dstMemory := deviceMemMap[dstId]
			reduceDeviceMemAddrs = append(reduceDeviceMemAddrs, &pb.DeviceMemAddr{
				DeviceId:   &pb.DeviceId{Value: srcId},
				SrcMemAddr: &pb.MemAddr{Value: srcMemory.gradientAddrs[i]},
				DstMemAddr: &pb.MemAddr{Value: dstMemory.gradientAddrs[i]},
				NumBytes:   srcMemory.gradientSizes[i],
			})
			i = (i + 1) % numDevices
		}
	}
	for offset := range numDevices - 1 {
		i := (numDevices - offset - 1) % numDevices
		for _, srcId := range deviceIds {
			dstId := (srcId + 1) % uint64(len(deviceIds))
			srcMemory := deviceMemMap[srcId]
			dstMemory := deviceMemMap[dstId]
			gatherDeviceMemAddrs = append(gatherDeviceMemAddrs, &pb.DeviceMemAddr{
				DeviceId:   &pb.DeviceId{Value: srcId},
				SrcMemAddr: &pb.MemAddr{Value: srcMemory.gradientAddrs[i]},
				DstMemAddr: &pb.MemAddr{Value: dstMemory.gradientAddrs[i]},
				NumBytes:   srcMemory.gradientSizes[i],
			})
			i = (i + 1) % numDevices
		}
	}
	return reduceDeviceMemAddrs, gatherDeviceMemAddrs
}

func main() {
	coordinatorAddress := "[::1]:8080" // hard coded based on gpu_config.json
	var numDevices uint32 = 4
	epochs := 64

	deviceMemMap := make(map[uint64]*Memory)

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

		deviceMemMap[deviceId] = &Memory{
			data:          &data,
			inputAddr:     inputAddr,
			inputSize:     inputSize,
			weightAddr:    weightAddr,
			weightSize:    weightSize,
			outputAddr:    outputAddr,
			outputSize:    outputSize,
			gradientAddrs: gradientAddrs,
			gradientSizes: gradientSizes,
		}
	}

	// Memcpy data over
	for _, deviceId := range deviceIds {
		memory := deviceMemMap[deviceId]
		hostToDeviceResponse, err := client.Memcpy(ctx, &pb.MemcpyRequest{
			Either: &pb.MemcpyRequest_HostToDevice{
				HostToDevice: &pb.MemcpyHostToDeviceRequest{
					HostSrcData: *memory.data,
					DstDeviceId: &pb.DeviceId{Value: deviceId},
					DstMemAddr:  &pb.MemAddr{Value: memory.inputAddr},
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

	for epoch := range epochs {
		forwardRequests := make(map[uint64]*pb.ForwardRequest)
		for deviceId, memory := range deviceMemMap {
			var gradientAddresses []*pb.MemAddr
			for _, gradientAddr := range memory.gradientAddrs {
				gradientAddresses = append(gradientAddresses, &pb.MemAddr{Value: gradientAddr})
			}
			forwardRequests[deviceId] = &pb.ForwardRequest{
				InputAddress:      &pb.MemAddr{Value: memory.inputAddr},
				InputSize:         memory.inputSize,
				WeightAddress:     &pb.MemAddr{Value: memory.weightAddr},
				WeightSize:        memory.weightSize,
				OutputAddress:     &pb.MemAddr{Value: memory.outputAddr},
				OutputSize:        memory.outputSize,
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
		reduceDeviceMemAddrs, gatherDeviceMemAddrs := computeDeviceMemAddrs(deviceIds, deviceMemMap, numDevices)
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
		}

		backwardsRequests := make(map[uint64]*pb.BackwardRequest)
		for deviceId, memory := range deviceMemMap {
			var gradientAddresses []*pb.MemAddr
			for _, gradientAddr := range memory.gradientAddrs {
				gradientAddresses = append(gradientAddresses, &pb.MemAddr{Value: gradientAddr})
			}
			backwardsRequests[deviceId] = &pb.BackwardRequest{
				WeightAddress:     &pb.MemAddr{Value: memory.weightAddr},
				WeightSize:        memory.weightSize,
				GradientAddresses: gradientAddresses,
				GradientSizes:     memory.gradientSizes,
				LearningRate:      1e-2 / float64(numDevices),
			}
		}
		client.BackwardBroadcast(
			ctx,
			&pb.BackwardBroadcastRequest{
				CommId:           commId,
				BackwardRequests: backwardsRequests,
			},
		)

		deviceId0 := deviceIds[0]
		memory0 := deviceMemMap[deviceId0]
		deviceToHostResponse, err := client.Memcpy(ctx, &pb.MemcpyRequest{
			Either: &pb.MemcpyRequest_DeviceToHost{
				DeviceToHost: &pb.MemcpyDeviceToHostRequest{
					SrcDeviceId: &pb.DeviceId{Value: deviceId0},
					SrcMemAddr:  &pb.MemAddr{Value: memory0.weightAddr},
					NumBytes:    uint64(memory0.weightSize),
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

		nSamples := testFeatures.RawMatrix().Rows
		nOutput := testTargets.RawMatrix().Cols

		predictions := utils.LinearSoftmax(testFeatures, W, nSamples, nOutput)
		testLoss := utils.CrossEntropy(predictions, testTargets, nSamples, nOutput)
		accuracy := utils.ComputeAccuracy(predictions, testTargets)

		log.Printf("epoch %v: training loss: %v | test loss: %v | test accuracy: %v", epoch, forwardResponse.Loss, testLoss, accuracy)
	}
}
