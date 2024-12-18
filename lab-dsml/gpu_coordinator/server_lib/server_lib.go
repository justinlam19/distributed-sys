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
	commDeviceClientMap map[uint64]map[uint64]*GPUDeviceClientInfo
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
		commDeviceClientMap: make(map[uint64]map[uint64]*GPUDeviceClientInfo),
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
	deviceClientsInfo := make(map[uint64]*GPUDeviceClientInfo)
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
		clientInfo := &GPUDeviceClientInfo{DeviceId: deviceId, Client: gpuDeviceClient, StreamIds: make(map[uint64][]uint64)}
		deviceClientsInfo[deviceId] = clientInfo
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
		for _, clientInfo := range deviceClientInfo {
			clientInfo.StreamIds[commId] = []uint64{}
		}
		return &pb.GetCommStatusResponse{Status: pb.Status_SUCCESS}, nil
	}
}

// group start and end are used to start nonblocking group operations
// but all operations implemented here are nonblocking by default
func (s *GPUCoordinatorService) GroupStart(ctx context.Context, req *pb.GroupStartRequest) (*pb.GroupStartResponse, error) {
	return &pb.GroupStartResponse{Success: true}, nil
}

// group start and end are used to start nonblocking group operations
// but all operations implemented here are nonblocking by default
func (s *GPUCoordinatorService) GroupEnd(ctx context.Context, req *pb.GroupEndRequest) (*pb.GroupEndResponse, error) {
	return &pb.GroupEndResponse{Success: true}, nil
}

func (s *GPUCoordinatorService) AllReduceRing(ctx context.Context, req *pb.AllReduceRingRequest) (*pb.AllReduceRingResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil request")
	}

	commId := req.CommId
	deviceClientsInfo, ok := s.commDeviceClientMap[commId]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid commId %v", commId)
	}

	numDevices := uint32(len(deviceClientsInfo))
	reduceDeviceMemAddrs := req.ReduceDeviceMemAddrs
	if len(reduceDeviceMemAddrs) != int(numDevices*(numDevices-1)) {
		return nil, status.Errorf(codes.InvalidArgument, "the number of memory addresses does not match the number of expected communications")
	}

	op := req.Op

	// Reduce for numDevices-1 rounds
	// #ranks = numDevices * (numDevices - 1)
	// each round, each device will send to the next device
	err := s.applyOpToRing(ctx, op, commId, reduceDeviceMemAddrs, deviceClientsInfo)
	if err != nil {
		return &pb.AllReduceRingResponse{Success: false}, status.Errorf(codes.Unknown, "failed all reduce ring in reduce stage")
	}

	// gather for numDevices-1 rounds
	// NOP causes the receiving device to replace the original chunk with the new
	// gather addresses are different because the address starting chunk that a device sends in reduce
	//  is not the same as the address of the chunk it ends up having a complete version
	gatherDeviceMemAddrs := req.GatherDeviceMemAddrs
	if len(reduceDeviceMemAddrs) != int(numDevices*(numDevices-1)) {
		return nil, status.Errorf(codes.InvalidArgument, "the number of memory addresses does not match the number of expected communications")
	}
	err = s.applyOpToRing(ctx, pb.ReduceOp_NOP, commId, gatherDeviceMemAddrs, deviceClientsInfo)
	if err != nil {
		return &pb.AllReduceRingResponse{Success: false}, err
	}

	return &pb.AllReduceRingResponse{Success: true}, nil
}

func (s *GPUCoordinatorService) applyOpToRing(
	ctx context.Context,
	op pb.ReduceOp,
	commId uint64,
	deviceMemAddrs []*pb.DeviceMemAddr,
	deviceClientsInfo map[uint64]*GPUDeviceClientInfo,
) error {
	for thisRank, deviceMemAddr := range deviceMemAddrs {
		nextRank := (thisRank + 1) % len(deviceMemAddrs) // next in ring
		thisDeviceId := deviceMemAddr.DeviceId.Value
		nextDeviceId := deviceMemAddrs[nextRank].DeviceId.Value
		srcDevice := deviceClientsInfo[thisDeviceId]
		dstDevice := deviceClientsInfo[nextDeviceId]

		// beginSend
		beginSendResponse, err := srcDevice.Client.BeginSend(ctx, &pb.BeginSendRequest{
			SendBuffAddr: deviceMemAddr.SrcMemAddr,
			NumBytes:     deviceMemAddr.NumBytes,
			DstDeviceId:  &pb.DeviceId{Value: nextDeviceId},
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
			RecvBuffAddr: deviceMemAddr.DstMemAddr,
			NumBytes:     deviceMemAddr.NumBytes,
			SrcDeviceId:  &pb.DeviceId{Value: thisDeviceId},
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

func (s *GPUCoordinatorService) ForwardBroadcast(ctx context.Context, req *pb.ForwardBroadcastRequest) (*pb.ForwardBroadcastResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil request")
	}
	commId := req.CommId
	deviceClientsInfo, ok := s.commDeviceClientMap[commId]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid commId %v", commId)
	}
	numDevices := uint32(len(deviceClientsInfo))
	loss := float64(0)
	for deviceId, forwardRequest := range req.ForwardRequests {
		device := deviceClientsInfo[deviceId]
		forwardResponse, err := device.Client.Forward(ctx, forwardRequest)
		if err != nil {
			return &pb.ForwardBroadcastResponse{Success: false}, status.Errorf(codes.Internal, "forward pass for gpu %v failed: %v", deviceId, err)
		}
		if !forwardResponse.Success {
			return &pb.ForwardBroadcastResponse{Success: false}, status.Errorf(codes.Internal, "forward pass for gpu %v failed", deviceId)
		}
		loss += forwardResponse.Loss
	}
	loss /= float64(numDevices)
	return &pb.ForwardBroadcastResponse{Success: true, Loss: loss}, nil
}

func (s *GPUCoordinatorService) BackwardBroadcast(ctx context.Context, req *pb.BackwardBroadcastRequest) (*pb.BackwardBroadcastResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil request")
	}
	commId := req.CommId
	deviceClientsInfo, ok := s.commDeviceClientMap[commId]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid commId %v", commId)
	}
	for deviceId, backwardRequest := range req.BackwardRequests {
		device := deviceClientsInfo[deviceId]
		backwardResponse, err := device.Client.Backward(ctx, backwardRequest)
		if err != nil {
			return &pb.BackwardBroadcastResponse{Success: false}, status.Errorf(codes.Internal, "backward pass for gpu %v failed: %v", deviceId, err)
		}
		if !backwardResponse.Success {
			return &pb.BackwardBroadcastResponse{Success: false}, status.Errorf(codes.Internal, "backward pass for gpu %v failed", deviceId)
		}
	}
	return &pb.BackwardBroadcastResponse{Success: true}, nil
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

		device.MemoryLock.RLock()
		defer device.MemoryLock.RUnlock()
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

		device.MemoryLock.Lock()
		defer device.MemoryLock.Unlock()
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
