package server_lib

import (
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
	utils "cs426.yale.edu/lab-dsml/utils"
	"gonum.org/v1/gonum/mat"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type GPUDeviceConfig struct {
	Address        string        `json:"address"`
	MemoryFilePath string        `json:"memory_file_path"`
	MemoryLock     *sync.RWMutex `json:"-"`
}

type GPUDeviceService struct {
	pb.UnimplementedGPUDeviceServer
	Id             uint64
	Address        string
	MemoryFilePath string
	MinAddr        uint64
	MaxAddr        uint64
	MemoryLock     *sync.RWMutex
	Config         map[uint64]*GPUDeviceConfig

	StreamIdCounter atomic.Uint64

	StreamStatusLock sync.RWMutex
	StreamStatuses   map[uint64]pb.Status

	SendStreamQueue ConcurrentStreamInfoQueue

	RcvStreamLock sync.RWMutex
	RcvStreamMap  map[uint64]*StreamInfo

	closeChan chan struct{}
}

func NewGPUDeviceService(config map[uint64]*GPUDeviceConfig, id uint64, minAddr uint64, maxAddr uint64) *GPUDeviceService {
	s := GPUDeviceService{
		Id:              id,
		Address:         config[id].Address,
		MemoryFilePath:  config[id].MemoryFilePath,
		MemoryLock:      config[id].MemoryLock,
		MinAddr:         minAddr,
		MaxAddr:         maxAddr,
		Config:          config,
		StreamStatuses:  make(map[uint64]pb.Status),
		SendStreamQueue: NewConcurrentStreamInfoQueue(),
		RcvStreamMap:    make(map[uint64]*StreamInfo),
		closeChan:       make(chan struct{}),
	}
	go s.repeatSend()
	return &s
}

func (s *GPUDeviceService) Close() {
	close(s.closeChan)
}

func (s *GPUDeviceService) GetDeviceMetadata(ctx context.Context, req *pb.GetDeviceMetadataRequest) (*pb.GetDeviceMetadataResponse, error) {
	return &pb.GetDeviceMetadataResponse{
		Metadata: &pb.DeviceMetadata{
			DeviceId:   &pb.DeviceId{Value: s.Id},
			MinMemAddr: &pb.MemAddr{Value: s.MinAddr},
			MaxMemAddr: &pb.MemAddr{Value: s.MaxAddr},
		},
	}, nil
}

func (s *GPUDeviceService) BeginSend(ctx context.Context, req *pb.BeginSendRequest) (*pb.BeginSendResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil request")
	}
	streamId := s.StreamIdCounter.Add(1)
	s.StreamStatusLock.Lock()
	s.StreamStatuses[streamId] = pb.Status_IN_PROGRESS
	s.StreamStatusLock.Unlock()

	s.SendStreamQueue.Enqueue(&StreamInfo{
		StreamId: streamId,
		Op:       pb.ReduceOp_NOP, // doesn't matter because this won't be used
		MemAddr:  req.SendBuffAddr.Value,
		NumBytes: req.NumBytes,
		SrcId:    s.Id,
		DstId:    req.DstDeviceId.Value,
	})
	return &pb.BeginSendResponse{
		Initiated: true,
		StreamId:  &pb.StreamId{Value: streamId},
	}, nil
}

// it needs to set up the memory spot
// set up the processing op
func (s *GPUDeviceService) BeginReceive(ctx context.Context, req *pb.BeginReceiveRequest) (*pb.BeginReceiveResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil request")
	}
	streamId := req.StreamId.Value
	op := req.Op
	memAddr := req.RecvBuffAddr.Value
	numBytes := req.NumBytes
	srcId := req.SrcDeviceId.Value

	s.RcvStreamLock.Lock()
	defer s.RcvStreamLock.Unlock()
	s.RcvStreamMap[streamId] = &StreamInfo{
		StreamId: streamId,
		Op:       op,
		MemAddr:  memAddr,
		NumBytes: numBytes,
		SrcId:    srcId,
		DstId:    s.Id,
	}
	return &pb.BeginReceiveResponse{Initiated: true}, nil
}

func (s *GPUDeviceService) repeatSend() {
	for {
		select {
		case <-s.closeChan:
			return
		case <-time.After(20 * time.Millisecond):
			if !s.SendStreamQueue.Empty() {
				streamInfo := s.SendStreamQueue.Dequeue()
				streamId := streamInfo.StreamId
				err := s.handleSend(streamInfo)
				s.StreamStatusLock.Lock()
				if err != nil {
					s.StreamStatuses[streamId] = pb.Status_FAILED
				} else if s.StreamStatuses[streamId] != pb.Status_FAILED {
					s.StreamStatuses[streamId] = pb.Status_SUCCESS
				}
				s.StreamStatusLock.Unlock()
			}
		}
	}
}

func (s *GPUDeviceService) handleSend(streamInfo *StreamInfo) error {
	memAddr := streamInfo.MemAddr
	numBytes := streamInfo.NumBytes

	s.MemoryLock.RLock()
	memoryFilePath := s.MemoryFilePath
	memoryFile, err := os.Open(memoryFilePath)
	if err != nil {
		s.MemoryLock.RUnlock()
		return err
	}
	buffer, err := readFile(memoryFile, memAddr, numBytes)
	if err != nil {
		memoryFile.Close()
		s.MemoryLock.RUnlock()
		return status.Errorf(codes.Unavailable, "failed to read file %v: %v", memoryFilePath, err)
	}
	memoryFile.Close()
	s.MemoryLock.RUnlock()

	dstId := streamInfo.DstId
	dstGPUInfo := s.Config[dstId]
	dstAddress := dstGPUInfo.Address
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	deviceConn, err := grpc.NewClient(dstAddress, opts...)
	if err != nil {
		return err
	}
	defer deviceConn.Close()

	dstGPUClient := pb.NewGPUDeviceClient(deviceConn)
	dstStreamClient, err := dstGPUClient.StreamSend(context.Background())
	if err != nil {
		return err
	}

	err = dstStreamClient.Send(&pb.DataChunk{Data: buffer, StreamId: &pb.StreamId{Value: streamInfo.StreamId}})
	if err != nil {
		return err
	}
	response, err := dstStreamClient.CloseAndRecv()
	if err != nil {
		return err
	}
	if !response.Success {
		return status.Errorf(codes.Unknown, "failed send to GPU %v", dstId)
	}
	return nil
}

// it receives data and does the processing
func (s *GPUDeviceService) StreamSend(streamServer grpc.ClientStreamingServer[pb.DataChunk, pb.StreamSendResponse]) error {
	for {
		dataChunk, err := streamServer.Recv()
		if err == io.EOF {
			// End of stream; client has finished sending data
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive data: %v", err)
		}
		if dataChunk == nil {
			return status.Errorf(codes.InvalidArgument, "received nil data")
		}

		data := dataChunk.Data
		streamId := dataChunk.StreamId.Value

		ok := false
		tries := 5
		var streamInfo *StreamInfo
		for !ok && tries > 0 {
			s.RcvStreamLock.RLock()
			streamInfo, ok = s.RcvStreamMap[streamId]
			s.RcvStreamLock.RUnlock()
			time.Sleep(50 * time.Millisecond)
		}
		if !ok {
			return status.Errorf(codes.InvalidArgument, "received nonexistent stream Id")
		}

		err = s.handleRcv(data, streamInfo)
		if err != nil {
			return err
		}
	}

	// Send a response to indicate success
	return streamServer.SendAndClose(&pb.StreamSendResponse{Success: true})
}

func (s *GPUDeviceService) handleRcv(data []byte, streamInfo *StreamInfo) error {
	memAddr := streamInfo.MemAddr
	numBytes := streamInfo.NumBytes
	op := streamInfo.Op

	s.MemoryLock.Lock()
	defer s.MemoryLock.Unlock()
	memoryFilePath := s.MemoryFilePath
	memoryFile, err := os.OpenFile(memoryFilePath, os.O_RDWR, 0666)
	if err != nil {
		return status.Errorf(codes.Unavailable, "failed to open file %v: %v", memoryFilePath, err)
	}
	defer memoryFile.Close()

	originalData, err := readFile(memoryFile, memAddr, numBytes)
	if err != nil {
		return status.Errorf(codes.Unavailable, "failed to read file %v: %v", memoryFilePath, err)
	}
	newData, err := s.executeOp(op, originalData, data)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to exeucte op %v: %v", op, err)
	}
	err = writeFile(memoryFile, memAddr, newData)
	if err != nil {
		return status.Errorf(codes.Unavailable, "failed to write to file %v: %v", memoryFilePath, err)
	}

	return nil
}

func (s *GPUDeviceService) executeOp(op pb.ReduceOp, originalData []byte, newData []byte) ([]byte, error) {
	a, err := utils.DeserializeMatrix(originalData)
	if err != nil {
		return []byte{}, status.Errorf(codes.FailedPrecondition, "could not deserialize original data to matrix")
	}
	b, err := utils.DeserializeMatrix(newData)
	if err != nil {
		return []byte{}, status.Errorf(codes.InvalidArgument, "could not deserialize new data to matrix")
	}

	var result *mat.Dense
	if op == pb.ReduceOp_MAX {
		result, err = utils.MaxMatrix(a, b)
		if err != nil {
			return []byte{}, status.Errorf(codes.InvalidArgument, "could not get max matrix: %v", err)
		}
	} else if op == pb.ReduceOp_MIN {
		result, err = utils.MinMatrix(a, b)
		if err != nil {
			return []byte{}, status.Errorf(codes.InvalidArgument, "could not get min matrix: %v", err)
		}
	} else if op == pb.ReduceOp_PROD {
		result, err = utils.ProdMatrix(a, b)
		if err != nil {
			return []byte{}, status.Errorf(codes.InvalidArgument, "could not multiply matrices: %v", err)
		}
		rR, cR := result.Dims()
		rA, cA := a.Dims()
		if rA != rR || cA != cR {
			return nil, status.Errorf(codes.InvalidArgument, "output matrix has different dimension from original")
		}
	} else if op == pb.ReduceOp_SUM {
		result, err = utils.SumMatrix(a, b)
		if err != nil {
			return []byte{}, status.Errorf(codes.InvalidArgument, "could not sum matrices: %v", err)
		}
	} else if op == pb.ReduceOp_NOP {
		return newData, nil
	}

	serializedResult, err := utils.SerializeMatrix(result)
	if err != nil {
		return []byte{}, status.Errorf(codes.InvalidArgument, "could not serialize result to bytes")
	}
	return serializedResult, nil
}

func (s *GPUDeviceService) GetStreamStatus(ctx context.Context, req *pb.GetStreamStatusRequest) (*pb.GetStreamStatusResponse, error) {
	streamIdRef := req.GetStreamId()
	if streamIdRef == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil stream id")
	}
	streamId := streamIdRef.GetValue()
	s.StreamStatusLock.RLock()
	streamStatus, ok := s.StreamStatuses[streamId]
	s.StreamStatusLock.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "stream id %v does not exist", streamId)
	}
	return &pb.GetStreamStatusResponse{Status: streamStatus}, nil
}

func (s *GPUDeviceService) Forward(ctx context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil request")
	}
	switch req.ForwardOp {
	case pb.ForwardOp_LINEAR_CROSS_ENTROPY:
		inputAddr := req.InputAddress.Value
		inputBytes := req.InputSize
		weightAddr := req.WeightAddress.Value
		weightBytes := req.WeightSize
		targetAddr := req.OutputAddress.Value
		targetBytes := req.OutputSize
		gradientAddresses := req.GradientAddresses

		s.MemoryLock.Lock()
		defer s.MemoryLock.Unlock()
		memoryFilePath := s.MemoryFilePath
		memoryFile, err := os.OpenFile(memoryFilePath, os.O_RDWR, 0666)
		if err != nil {
			return &pb.ForwardResponse{Success: false}, status.Errorf(codes.Unavailable, "failed to open file %v: %v", memoryFilePath, err)
		}
		defer memoryFile.Close()

		X, err := readMatrix(memoryFile, inputAddr, inputBytes)
		if err != nil {
			return &pb.ForwardResponse{Success: false}, status.Errorf(codes.Unavailable, "failed to load input: %v", err)
		}
		W, err := readMatrix(memoryFile, weightAddr, weightBytes)
		if err != nil {
			return &pb.ForwardResponse{Success: false}, status.Errorf(codes.Unavailable, "failed to load weight: %v", err)
		}
		Y, err := readMatrix(memoryFile, targetAddr, targetBytes)
		if err != nil {
			return &pb.ForwardResponse{Success: false}, status.Errorf(codes.Unavailable, "failed to load target output: %v", err)
		}

		grad, loss, err := utils.LinearCrossEntropyGradients(X, W, Y)
		if err != nil {
			return &pb.ForwardResponse{Success: false}, status.Errorf(codes.InvalidArgument, "failed to compute gradients: %v", err)
		}
		gradChunks := utils.SplitMatrix(grad, len(gradientAddresses))

		var gradBytesChunks [][]byte
		for _, chunk := range gradChunks {
			bytes, err := utils.SerializeMatrix(chunk)
			if err != nil {
				return &pb.ForwardResponse{Success: false}, status.Errorf(codes.Unavailable, "failed to serialize gradient chunk: %v", err)
			}
			gradBytesChunks = append(gradBytesChunks, bytes)
		}

		for i, gradientAddr := range gradientAddresses {
			err = writeFile(memoryFile, gradientAddr.Value, gradBytesChunks[i])
			if err != nil {
				return &pb.ForwardResponse{Success: false}, status.Errorf(codes.Unavailable, "failed to write gradient chunk to memory: %v", err)
			}
		}
		return &pb.ForwardResponse{Success: true, Loss: loss}, nil

	default:
		return &pb.ForwardResponse{Success: false}, status.Errorf(codes.Unimplemented, "unsupported forward operation")
	}
}

func (s *GPUDeviceService) Backward(ctx context.Context, req *pb.BackwardRequest) (*pb.BackwardResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil request")
	}

	s.MemoryLock.Lock()
	defer s.MemoryLock.Unlock()
	memoryFilePath := s.MemoryFilePath
	memoryFile, err := os.OpenFile(memoryFilePath, os.O_RDWR, 0666)
	if err != nil {
		return &pb.BackwardResponse{Success: false}, status.Errorf(codes.Unavailable, "failed to open file %v: %v", memoryFilePath, err)
	}
	defer memoryFile.Close()

	W, err := readMatrix(memoryFile, req.WeightAddress.Value, req.WeightSize)
	if err != nil {
		return &pb.BackwardResponse{Success: false}, status.Errorf(codes.InvalidArgument, "failed to load weight: %v", err)
	}
	var gradients []*mat.Dense
	for i, gradientAddr := range req.GradientAddresses {
		grad, err := readMatrix(memoryFile, gradientAddr.Value, req.GradientSizes[i])
		if err != nil {
			return &pb.BackwardResponse{Success: false}, status.Errorf(codes.InvalidArgument, "failed to load gradient chunk: %v", err)
		}
		gradients = append(gradients, grad)
	}
	mergedGrads, err := utils.MergeMatrix(gradients)
	if err != nil {
		return &pb.BackwardResponse{Success: false}, status.Errorf(codes.InvalidArgument, "failed to merge gradients: %v", err)
	}
	newW, err := utils.ComputeNewWeights(W, mergedGrads, req.LearningRate)
	if err != nil {
		return &pb.BackwardResponse{Success: false}, status.Errorf(codes.InvalidArgument, "failed to compute new weight: %v", err)
	}
	serializedNewWeight, err := utils.SerializeMatrix(newW)
	if err != nil {
		return &pb.BackwardResponse{Success: false}, status.Errorf(codes.InvalidArgument, "failed to serialize new weight: %v", err)
	}
	err = writeFile(memoryFile, req.WeightAddress.Value, serializedNewWeight)
	if err != nil {
		return &pb.BackwardResponse{Success: false}, status.Errorf(codes.InvalidArgument, "failed to write new weightto memory: %v", err)
	}
	return &pb.BackwardResponse{Success: true}, nil
}

func readFile(file *os.File, memAddr uint64, numBytes uint64) ([]byte, error) {
	_, err := file.Seek(int64(memAddr), 0)
	if err != nil {
		return []byte{}, status.Errorf(codes.NotFound, "failed to get memory address %v: %v", memAddr, err)
	}
	buffer := make([]byte, numBytes)
	_, err = file.Read(buffer)
	if err != nil {
		return []byte{}, status.Errorf(codes.NotFound, "failed to read from memory address %v: %v", memAddr, err)
	}
	return buffer, nil
}

func writeFile(file *os.File, memAddr uint64, data []byte) error {
	_, err := file.Seek(int64(memAddr), 0)
	if err != nil {
		return status.Errorf(codes.NotFound, "failed to get memory address %v: %v", memAddr, err)
	}
	_, err = file.Write(data)
	if err != nil {
		return status.Errorf(codes.NotFound, "failed to write to memory address %v: %v", memAddr, err)
	}
	return nil
}

func readMatrix(file *os.File, memAddr uint64, numBytes uint64) (*mat.Dense, error) {
	data, err := readFile(file, memAddr, numBytes)
	if err != nil {
		return nil, err
	}
	return utils.DeserializeMatrix(data)
}
