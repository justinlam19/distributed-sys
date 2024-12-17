package mock

import (
	"context"

	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type MockGPUDeviceClient struct {
	mock.Mock
}

func (m *MockGPUDeviceClient) GetDeviceMetadata(ctx context.Context, in *pb.GetDeviceMetadataRequest, opts ...grpc.CallOption) (*pb.GetDeviceMetadataResponse, error) {
	args := m.Called(ctx, in)
	if res := args.Get(0); res != nil {
		return res.(*pb.GetDeviceMetadataResponse), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockGPUDeviceClient) BeginSend(ctx context.Context, req *pb.BeginSendRequest, opts ...grpc.CallOption) (*pb.BeginSendResponse, error) {
	args := m.Called(ctx, req)
	if res := args.Get(0); res != nil {
		return res.(*pb.BeginSendResponse), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockGPUDeviceClient) BeginReceive(ctx context.Context, req *pb.BeginReceiveRequest, opts ...grpc.CallOption) (*pb.BeginReceiveResponse, error) {
	args := m.Called(ctx, req)
	if res := args.Get(0); res != nil {
		return res.(*pb.BeginReceiveResponse), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockGPUDeviceClient) StreamSend(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[pb.DataChunk, pb.StreamSendResponse], error) {
	args := m.Called(ctx)
	if res := args.Get(0); res != nil {
		return res.(grpc.ClientStreamingClient[pb.DataChunk, pb.StreamSendResponse]), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockGPUDeviceClient) GetStreamStatus(ctx context.Context, req *pb.GetStreamStatusRequest, opts ...grpc.CallOption) (*pb.GetStreamStatusResponse, error) {
	args := m.Called(ctx, req)
	if res := args.Get(0); res != nil {
		return res.(*pb.GetStreamStatusResponse), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockGPUDeviceClient) Forward(ctx context.Context, req *pb.ForwardRequest, opts ...grpc.CallOption) (*pb.ForwardResponse, error) {
	args := m.Called(ctx, req)
	if res := args.Get(0); res != nil {
		return res.(*pb.ForwardResponse), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockGPUDeviceClient) Backward(ctx context.Context, req *pb.BackwardRequest, opts ...grpc.CallOption) (*pb.BackwardResponse, error) {
	args := m.Called(ctx, req)
	if res := args.Get(0); res != nil {
		return res.(*pb.BackwardResponse), args.Error(1)
	}
	return nil, args.Error(1)
}
