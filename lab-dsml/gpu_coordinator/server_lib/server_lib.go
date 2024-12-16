package server_lib

//NOT threadsafe

import (
	"context"
	"os"

	"math/rand"

	gpu "cs426.yale.edu/lab-dsml/gpu_device/server_lib"
	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type GPUDeviceClientInfo struct {
	DeviceId  uint64
	Client    pb.GPUDeviceClient
	StreamIds map[uint64][]uint64
}

type GPUCoordinatorService struct {
	pb.UnimplementedGPUCoordinatorServer
	config              map[uint64]*gpu.GPUDeviceConfig
	deviceIds           []uint64
	deviceCounter       uint64
	commIDCounter       uint64
	commDeviceClientMap map[uint64][]*GPUDeviceClientInfo
}

func NewGPUCoordinatorService(config map[uint64]*gpu.GPUDeviceConfig) *GPUCoordinatorService {
	var deviceIds []uint64
	for id := range config {
		deviceIds = append(deviceIds, id)
	}
	return &GPUCoordinatorService{
		config:              config,
		deviceIds:           deviceIds,
		deviceCounter:       uint64(rand.Intn(len(config))),
		commIDCounter:       0,
		commDeviceClientMap: make(map[uint64][]*GPUDeviceClientInfo),
	}
}

func (s *GPUCoordinatorService) CommInit(ctx context.Context, req *pb.CommInitRequest) (*pb.CommInitResponse, error) {
	numDevices := req.GetNumDevices()
	if numDevices == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "number of devices cannot be 0")
	}
	if int(numDevices) > len(s.config) {
		return nil, status.Errorf(codes.InvalidArgument, "number of devices requested exceeds those available")
	}

	commId := s.commIDCounter
	s.commIDCounter++

	var devices []*pb.DeviceMetadata
	var deviceClientsInfo []*GPUDeviceClientInfo
	for range numDevices {
		// choose device by round robin (deviceCounter is randomly initialized)
		deviceIndex := s.deviceCounter % uint64(len(s.deviceIds))
		s.deviceCounter++
		deviceId := s.deviceIds[deviceIndex]
		addr := s.config[deviceId].Address

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		deviceConn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			return &pb.CommInitResponse{
				Success: false,
			}, status.Errorf(codes.NotFound, "error initializing connection to GPU device %v: %v", deviceId, err)
		}

		gpuDeviceClient := pb.NewGPUDeviceClient(deviceConn)
		clientInfo := &GPUDeviceClientInfo{DeviceId: deviceId, Client: gpuDeviceClient}
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
		streamIds, ok := clientInfo.StreamIds[commId]
		if !ok {
			return nil, status.Errorf(codes.Internal, "couldn't find stream ids for commId %v", commId)
		}
		for _, streamId := range streamIds {
			streamStatusResponse, err := clientInfo.Client.GetStreamStatus(
				ctx,
				&pb.GetStreamStatusRequest{StreamId: &pb.StreamId{Value: streamId}},
			)
			if err != nil {
				return nil, status.Errorf(codes.NotFound, "unable to get stream status of %v: %v", streamId, err)
			}
			streamStatus := streamStatusResponse.GetStatus()
			if streamStatus == pb.Status_FAILED {
				return &pb.GetCommStatusResponse{Status: streamStatus}, nil
			} else if streamStatus == pb.Status_IN_PROGRESS {
				inProgress = true
			}
		}
	}

	if inProgress {
		return &pb.GetCommStatusResponse{Status: pb.Status_IN_PROGRESS}, nil
	} else {
		return &pb.GetCommStatusResponse{Status: pb.Status_SUCCESS}, nil
	}
}

func (s *GPUCoordinatorService) GroupStart(ctx context.Context, req *pb.GroupStartRequest) (*pb.GroupStartResponse, error) {
	return &pb.GroupStartResponse{Success: true}, nil
}

func (s *GPUCoordinatorService) GroupEnd(ctx context.Context, req *pb.GroupEndRequest) (*pb.GroupEndResponse, error) {
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

	numDevices := uint32(len(s.commDeviceClientMap[commId]))
	if len(memAddrs) != int(numDevices*(numDevices-1)) {
		return nil, status.Errorf(codes.InvalidArgument, "the number of memory addresses does not match the number of devices")
	}

	op := req.GetOp()
	numBytes := uint64(req.GetCount())

	// Reduce for numDevices-1 rounds
	// #ranks = numDevices * (numDevices - 1)
	// each round, each device will send to the next device
	err := s.applyOpToRing(ctx, op, commId, numBytes, numDevices, memAddrs, deviceClientsInfo)
	if err != nil {
		return &pb.AllReduceRingResponse{Success: false}, err
	}

	// gather for numDevices-1 rounds
	// NOP causes the receiving device to replace the original chunk with the new
	err = s.applyOpToRing(ctx, pb.ReduceOp_NOP, commId, numBytes, numDevices, memAddrs, deviceClientsInfo)
	if err != nil {
		return &pb.AllReduceRingResponse{Success: false}, err
	}

	return &pb.AllReduceRingResponse{Success: true}, nil
}

func (s *GPUCoordinatorService) applyOpToRing(
	ctx context.Context,
	op pb.ReduceOp,
	commId uint64,
	numBytes uint64,
	numDevices uint32,
	memAddrs map[uint32]*pb.MemAddr,
	deviceClientsInfo []*GPUDeviceClientInfo,
) error {
	for rank, memAddr := range memAddrs {
		thisRank := rank % numDevices           // there are multiple ranks because each corresponds to a single communication
		nextRank := (thisRank + 1) % numDevices // next in ring
		srcDevice := deviceClientsInfo[thisRank]
		dstDevice := deviceClientsInfo[nextRank]

		// beginSend
		beginSendResponse, err := srcDevice.Client.BeginSend(ctx, &pb.BeginSendRequest{
			SendBuffAddr: memAddr,
			NumBytes:     numBytes,
			DstDeviceId:  &pb.DeviceId{Value: dstDevice.DeviceId},
		})
		if err != nil {
			return status.Errorf(codes.Unavailable, "failed begin send for rank %v to %v: %v", thisRank, nextRank, err)
		}
		if beginSendResponse == nil || !beginSendResponse.GetInitiated() {
			return status.Errorf(codes.Unavailable, "failed begin send for rank %v to %v", thisRank, nextRank)
		}

		// save stream id for future querying
		streamId := beginSendResponse.GetStreamId()
		_, ok := dstDevice.StreamIds[commId]
		if !ok {
			dstDevice.StreamIds[commId] = []uint64{streamId.Value}
		} else {
			dstDevice.StreamIds[commId] = append(dstDevice.StreamIds[commId], streamId.Value)
		}

		// beginReceive
		beginReceiveResponse, err := dstDevice.Client.BeginReceive(ctx, &pb.BeginReceiveRequest{
			StreamId:     streamId,
			RecvBuffAddr: memAddr,
			NumBytes:     numBytes,
			SrcDeviceId:  &pb.DeviceId{Value: srcDevice.DeviceId},
			Op:           op,
		})
		if err != nil {
			return status.Errorf(codes.Unavailable, "failed begin receive for rank %v from %v: %v", nextRank, thisRank, err)
		}
		if beginReceiveResponse == nil || !beginReceiveResponse.GetInitiated() {
			return status.Errorf(codes.Unavailable, "failed begin receive for rank %v from %v", nextRank, thisRank)
		}
	}
	return nil
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
		deviceFile, err := os.OpenFile(deviceFilePath, os.O_WRONLY, 0666)
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
			return failureResponse, status.Errorf(codes.NotFound, "failed to write to memory address %v: %v", memAddr, err)
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
