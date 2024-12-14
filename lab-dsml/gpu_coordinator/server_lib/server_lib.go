package server_lib

//NOT threadsafe

import (
	"context"
	"os"

	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type GPUDeviceConfig struct {
	Address        string
	MemoryFilePath string
}

type GPUDeviceClientInfo struct {
	Client   pb.GPUDeviceClient
	StreamId *uint64
}

type GPUCoordinatorService struct {
	pb.UnimplementedGPUCoordinatorServer
	config              map[uint64]*GPUDeviceConfig
	deviceCounter       uint64
	commIDCounter       uint64
	commDeviceClientMap map[uint64][]*GPUDeviceClientInfo
}

func NewGPUCoordinatorService(config map[uint64]*GPUDeviceConfig) *GPUCoordinatorService {
	return &GPUCoordinatorService{
		config: config,
	}
}

func serviceConn(address string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return grpc.NewClient(address, opts...)
}

func (s *GPUCoordinatorService) CommInit(ctx context.Context, req *pb.CommInitRequest) (*pb.CommInitResponse, error) {
	numDevices := req.GetNumDevices()
	if numDevices == 0 {
		return &pb.CommInitResponse{
			Success: false,
		}, status.Errorf(codes.InvalidArgument, "number of devices cannot be 0")
	}
	if int(numDevices) > len(s.config) {
		return &pb.CommInitResponse{
			Success: false,
		}, status.Errorf(codes.InvalidArgument, "number of devices requested exceeds those available")
	}

	commId := s.commIDCounter
	s.commIDCounter++
	var devices []*pb.DeviceMetadata
	var deviceClientsInfo []*GPUDeviceClientInfo
	for range numDevices {
		deviceId := s.deviceCounter % uint64(len(s.config))
		s.deviceCounter++
		addr := s.config[deviceId].Address

		deviceConn, err := serviceConn(addr)
		if err != nil {
			return &pb.CommInitResponse{
				Success: false,
			}, status.Errorf(codes.NotFound, "error initializing connection to GPU device %v: %v", deviceId, err)
		}

		gpuDeviceClient := pb.NewGPUDeviceClient(deviceConn)
		clientInfo := &GPUDeviceClientInfo{Client: gpuDeviceClient}
		deviceClientsInfo = append(deviceClientsInfo, clientInfo)
		metadataResponse, err := gpuDeviceClient.GetDeviceMetadata(ctx, &pb.GetDeviceMetadataRequest{})
		if err != nil {
			return &pb.CommInitResponse{
				Success: false,
			}, status.Errorf(codes.NotFound, "error getting metadata from GPU device %v: %v", deviceId, err)
		}

		metadata := metadataResponse.GetMetadata()
		if metadata == nil {
			return &pb.CommInitResponse{
				Success: false,
			}, status.Errorf(codes.NotFound, "error getting metadata from GPU device %v", deviceId)
		}
		devices = append(devices, metadata)
	}

	s.commDeviceClientMap[commId] = deviceClientsInfo
	return &pb.CommInitResponse{
		Success: true,
		CommId:  commId,
		Devices: devices,
	}, nil
}

func (s *GPUCoordinatorService) GetCommStatus(ctx context.Context, req *pb.GetCommStatusRequest) (*pb.GetCommStatusResponse, error) {
	commId := req.GetCommId()
	deviceClientInfo, ok := s.commDeviceClientMap[commId]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid commId %v", commId)
	}

	inProgress := false
	for _, clientInfo := range deviceClientInfo {
		streamId := clientInfo.StreamId
		if streamId == nil {
			continue
		}

		streamStatusResponse, err := clientInfo.Client.GetStreamStatus(
			ctx,
			&pb.GetStreamStatusRequest{StreamId: &pb.StreamId{Value: *streamId}},
		)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "unable to get stream status of %v: %v", *streamId, err)
		}

		streamStatus := streamStatusResponse.GetStatus()
		if streamStatus == pb.Status_FAILED {
			return &pb.GetCommStatusResponse{Status: streamStatus}, nil
		} else if streamStatus == pb.Status_IN_PROGRESS {
			inProgress = true
		}
	}

	if inProgress {
		return &pb.GetCommStatusResponse{Status: pb.Status_IN_PROGRESS}, nil
	} else {
		return &pb.GetCommStatusResponse{Status: pb.Status_SUCCESS}, nil
	}
}

func (s *GPUCoordinatorService) GroupStart(ctx context.Context, req *pb.GroupStartRequest) (*pb.GroupStartResponse, error) {
	commId := req.GetCommId()
	deviceClientInfo, ok := s.commDeviceClientMap[commId]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid commId %v", commId)
	}
	for _, clientInfo := range deviceClientInfo {
		streamId := clientInfo.StreamId
		if streamId != nil {
			return &pb.GroupStartResponse{Success: false}, nil
		}
	}
	return &pb.GroupStartResponse{Success: true}, nil
}

func (s *GPUCoordinatorService) GroupEnd(ctx context.Context, req *pb.GroupEndRequest) (*pb.GroupEndResponse, error) {
	commId := req.GetCommId()
	deviceClientInfo, ok := s.commDeviceClientMap[commId]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid commId %v", commId)
	}
	for _, clientInfo := range deviceClientInfo {
		clientInfo.StreamId = nil
	}
	return &pb.GroupEndResponse{Success: true}, nil
}

func (s *GPUCoordinatorService) AllReduceRing(ctx context.Context, req *pb.AllReduceRingRequest) (*pb.AllReduceRingResponse, error) {
	commId := req.GetCommId()
	deviceClientsInfo, ok := s.commDeviceClientMap[commId]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid commId %v", commId)
	}
	memAddrs := req.GetMemAddrs()
	if memAddrs == nil {
		return nil, status.Errorf(codes.InvalidArgument, "memory addresses cannot be nil")
	}

	// op := req.GetOp()
	numDevices := uint32(len(s.commDeviceClientMap[commId]))
	numBytes := uint64(req.GetCount())
	for rank, memAddr := range memAddrs {
		nextRank := (rank + 1) % numDevices
		beginSendResponse, err := deviceClientsInfo[rank].Client.BeginSend(ctx, &pb.BeginSendRequest{
			SendBuffAddr: memAddrs[nextRank],
			NumBytes:     numBytes,
			DstRank:      &pb.Rank{Value: nextRank},
		})
		if err != nil || !beginSendResponse.GetInitiated() {
			response := &pb.AllReduceRingResponse{Success: false}
			return response, status.Errorf(codes.Unavailable, "failed begin send for rank %v to %v: %v", rank, nextRank, err)
		}

		streamId := beginSendResponse.GetStreamId()
		deviceClientsInfo[rank].StreamId = &streamId.Value
		beginReceiveResponse, err := deviceClientsInfo[nextRank].Client.BeginReceive(ctx, &pb.BeginReceiveRequest{
			StreamId:     streamId,
			RecvBuffAddr: memAddr,
			NumBytes:     numBytes,
			SrcRank:      &pb.Rank{Value: rank},
		})
		if err != nil || !beginReceiveResponse.GetInitiated() {
			response := &pb.AllReduceRingResponse{Success: false}
			return response, status.Errorf(codes.Unavailable, "failed begin receive for rank %v from %v: %v", nextRank, rank, err)
		}
	}

	return &pb.AllReduceRingResponse{Success: true}, nil
}

func (s *GPUCoordinatorService) Memcpy(ctx context.Context, req *pb.MemcpyRequest) (*pb.MemcpyResponse, error) {
	either := req.GetEither()
	if either == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil request")
	}

	switch request := either.(type) {
	case *pb.MemcpyRequest_DeviceToHost:
		deviceToHostRequest := request.DeviceToHost
		deviceId := deviceToHostRequest.SrcDeviceId.GetValue()
		memAddr := deviceToHostRequest.SrcMemAddr.GetValue()
		numBytes := deviceToHostRequest.NumBytes

		device, ok := s.config[deviceId]
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "no device %v in config", deviceId)
		}

		deviceFilePath := device.MemoryFilePath
		deviceFile, err := os.Open(deviceFilePath)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed to open file %v: %v", deviceFilePath, err)
		}
		defer deviceFile.Close()

		_, err = deviceFile.Seek(int64(memAddr), 0)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "failed to get memory address %v: %v", memAddr, err)
		}

		buffer := make([]byte, numBytes)
		_, err = deviceFile.Read(buffer)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "failed to read from memory address %v: %v", memAddr, err)
		}

		return &pb.MemcpyResponse{
			Either: &pb.MemcpyResponse_DeviceToHost{
				DeviceToHost: &pb.MemcpyDeviceToHostResponse{DstData: buffer},
			},
		}, nil

	case *pb.MemcpyRequest_HostToDevice:
		hostToDeviceRequest := request.HostToDevice
		deviceId := hostToDeviceRequest.DstDeviceId.GetValue()
		memAddr := hostToDeviceRequest.DstMemAddr.GetValue()
		data := hostToDeviceRequest.HostSrcData

		device, ok := s.config[deviceId]
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "no device %v in config", deviceId)
		}

		failureResponse := &pb.MemcpyResponse{
			Either: &pb.MemcpyResponse_HostToDevice{
				HostToDevice: &pb.MemcpyHostToDeviceResponse{Success: false},
			},
		}

		deviceFilePath := device.MemoryFilePath
		deviceFile, err := os.Open(deviceFilePath)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed to open file %v: %v", deviceFilePath, err)
		}
		defer deviceFile.Close()

		_, err = deviceFile.Seek(int64(memAddr), 0)
		if err != nil {
			return failureResponse, status.Errorf(codes.NotFound, "failed to get memory address %v: %v", memAddr, err)
		}

		_, err = deviceFile.Write(data)
		if err != nil {
			return failureResponse, status.Errorf(codes.NotFound, "failed to read write to memory address %v: %v", memAddr, err)
		}

		return &pb.MemcpyResponse{
			Either: &pb.MemcpyResponse_HostToDevice{
				HostToDevice: &pb.MemcpyHostToDeviceResponse{Success: true},
			},
		}, nil

	default:
		return nil, status.Errorf(codes.InvalidArgument, "incorrect request type")
	}
}
