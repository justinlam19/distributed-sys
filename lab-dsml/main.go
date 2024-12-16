package main

import (
	"context"
	"log"

	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var trainData [][]byte

func main() {
	coordinatorAddress := ""
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(coordinatorAddress, opts...)
	if err != nil {
		log.Fatalf("failed to connect to GPU Coordinator Server: %v", err)
	}

	var numDevices uint32 = 4
	deviceMemMap := make(map[uint64]uint64)

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
	log.Printf("%v", commId)
	metaData := initResponse.Devices
	if metaData == nil {
		log.Fatalf("failed to retrieve metadata for devices")
	}

	for _, metaDatum := range metaData {
		device := metaDatum.DeviceId.Value
		memAddr := metaDatum.MinMemAddr.Value
		deviceMemMap[device] = memAddr
	}

	for i, metaDatum := range metaData {
		dataSubset := trainData[i]
		device := metaDatum.DeviceId.Value

		memAddr := deviceMemMap[device]
		deviceMemMap[device] += uint64(len(dataSubset))

		hostToDeviceResponse, err := client.Memcpy(ctx, &pb.MemcpyRequest{
			Either: &pb.MemcpyRequest_HostToDevice{
				HostToDevice: &pb.MemcpyHostToDeviceRequest{
					HostSrcData: dataSubset,
					DstDeviceId: &pb.DeviceId{Value: device},
					DstMemAddr:  &pb.MemAddr{Value: memAddr},
				},
			},
		})
		if err != nil {
			log.Fatalf("failed to memcpy to device %v", device)
		}
		switch r := hostToDeviceResponse.Either.(type) {
		case *pb.MemcpyResponse_HostToDevice:
			if !r.HostToDevice.Success {
				log.Fatalf("failed to memcpy to device %v", device)
			}
		default:
			log.Fatalf("received invalid response format when memcpy to device %v", device)
		}
	}

	groupStartResponse, err := client.GroupStart(ctx, &pb.GroupStartRequest{CommId: commId})
	if err != nil {
		log.Fatalf("failed group start on comm %v: %v", commId, err)
	}
	if !groupStartResponse.Success {
		log.Fatalf("failed group start on comm %v", commId)
	}

	memAddrs := make(map[uint32]*pb.MemAddr)
	i := uint32(0)
	for _, addr := range deviceMemMap {
		memAddrs[i] = &pb.MemAddr{Value: addr}
		i += 1
	}
	allReduceResponse, err := client.AllReduceRing(ctx, &pb.AllReduceRingRequest{
		CommId:   commId,
		Count:    uint64(calculateSizeOfData()),
		Op:       pb.ReduceOp_SUM,
		MemAddrs: memAddrs,
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

	var deviceId uint64
	for device := range deviceMemMap {
		deviceId = device
		break
	}
	deviceToHostResponse, err := client.Memcpy(ctx, &pb.MemcpyRequest{
		Either: &pb.MemcpyRequest_DeviceToHost{
			DeviceToHost: &pb.MemcpyDeviceToHostRequest{
				SrcDeviceId: &pb.DeviceId{Value: deviceId},
				SrcMemAddr:  &pb.MemAddr{Value: calculateMemoryAddress()},
				NumBytes:    uint64(calculateSizeOfData()),
			},
		},
	})
	if err != nil {
		log.Fatalf("failed to get data from device %v", deviceId)
	}

	var buffer []byte
	switch r := deviceToHostResponse.Either.(type) {
	case *pb.MemcpyResponse_DeviceToHost:
		buffer = r.DeviceToHost.DstData
	default:
		log.Fatalf("invalid response format when memcpy from device %v", deviceId)
	}

	log.Printf("%v", buffer)
}

func calculateSizeOfData() int {
	panic("NOT IMPLEMENTED")
}

func calculateMemoryAddress() uint64 {
	panic("NOT IMPLEMENTED")
}
