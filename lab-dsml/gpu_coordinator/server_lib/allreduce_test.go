package server_lib

import (
	"context"
	"testing"

	gpuMock "cs426.yale.edu/lab-dsml/gpu_device/mock"
	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestApplyOpToRing_Success(t *testing.T) {
	// Setup mock clients
	mockClient1 := new(gpuMock.MockGPUDeviceClient)
	mockClient2 := new(gpuMock.MockGPUDeviceClient)
	mockClient3 := new(gpuMock.MockGPUDeviceClient)

	// Create mock responses for BeginSend and BeginReceive
	mockClient1.On("BeginSend", mock.Anything, mock.Anything).Return(
		&pb.BeginSendResponse{Initiated: true, StreamId: &pb.StreamId{Value: 123}},
		nil,
	)
	mockClient1.On("BeginReceive", mock.Anything, mock.Anything).Return(
		&pb.BeginReceiveResponse{Initiated: true},
		nil,
	)

	mockClient2.On("BeginSend", mock.Anything, mock.Anything).Return(
		&pb.BeginSendResponse{Initiated: true, StreamId: &pb.StreamId{Value: 123}},
		nil,
	)
	mockClient2.On("BeginReceive", mock.Anything, mock.Anything).Return(
		&pb.BeginReceiveResponse{Initiated: true},
		nil,
	)

	mockClient3.On("BeginSend", mock.Anything, mock.Anything).Return(
		&pb.BeginSendResponse{Initiated: true, StreamId: &pb.StreamId{Value: 123}},
		nil,
	)
	mockClient3.On("BeginReceive", mock.Anything, mock.Anything).Return(
		&pb.BeginReceiveResponse{Initiated: true},
		nil,
	)

	// Define test parameters
	op := pb.ReduceOp_SUM
	commId := uint64(1)

	// Set up device clients information
	deviceClientsInfo := map[uint64]*GPUDeviceClientInfo{
		0: {DeviceId: 0, Client: mockClient1, StreamIds: map[uint64][]uint64{commId: {}}},
		1: {DeviceId: 1, Client: mockClient2, StreamIds: map[uint64][]uint64{commId: {}}},
		2: {DeviceId: 2, Client: mockClient3, StreamIds: map[uint64][]uint64{commId: {}}},
	}

	// Define memory addresses for communication
	deviceMemAddrs := []*pb.DeviceMemAddr{
		{DeviceId: &pb.DeviceId{Value: 0}, SrcMemAddr: &pb.MemAddr{Value: 0}, DstMemAddr: &pb.MemAddr{Value: 1}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 1}, SrcMemAddr: &pb.MemAddr{Value: 8}, DstMemAddr: &pb.MemAddr{Value: 2}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 2}, SrcMemAddr: &pb.MemAddr{Value: 16}, DstMemAddr: &pb.MemAddr{Value: 3}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 0}, SrcMemAddr: &pb.MemAddr{Value: 24}, DstMemAddr: &pb.MemAddr{Value: 4}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 1}, SrcMemAddr: &pb.MemAddr{Value: 32}, DstMemAddr: &pb.MemAddr{Value: 5}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 2}, SrcMemAddr: &pb.MemAddr{Value: 40}, DstMemAddr: &pb.MemAddr{Value: 6}, NumBytes: 10},
	}

	// Create the service
	service := &GPUCoordinatorService{}

	// Call the method
	err := service.applyOpToRing(context.Background(), op, commId, deviceMemAddrs, deviceClientsInfo)

	// Assertions
	assert.Nil(t, err)
	mockClient1.AssertExpectations(t)
	mockClient2.AssertExpectations(t)
	mockClient3.AssertExpectations(t)
}

func TestApplyOpToRing_BeginSendError(t *testing.T) {
	// Setup mock clients
	mockClient1 := new(gpuMock.MockGPUDeviceClient)
	mockClient2 := new(gpuMock.MockGPUDeviceClient)
	mockClient3 := new(gpuMock.MockGPUDeviceClient)

	// Simulate error in BeginSend
	mockClient1.On("BeginSend", mock.Anything, mock.Anything).Return(nil, status.Errorf(codes.Unavailable, "send failed"))

	mockClient1.On("BeginReceive", mock.Anything, mock.Anything).Return(&pb.BeginReceiveResponse{Initiated: true}, nil).Maybe()

	mockClient2.On("BeginSend", mock.Anything, mock.Anything).Return(&pb.BeginSendResponse{Initiated: true, StreamId: &pb.StreamId{Value: 123}}, nil).Maybe()
	mockClient2.On("BeginReceive", mock.Anything, mock.Anything).Return(&pb.BeginReceiveResponse{Initiated: true}, nil).Maybe()

	mockClient3.On("BeginSend", mock.Anything, mock.Anything).Return(&pb.BeginSendResponse{Initiated: true, StreamId: &pb.StreamId{Value: 123}}, nil).Maybe()
	mockClient3.On("BeginReceive", mock.Anything, mock.Anything).Return(&pb.BeginReceiveResponse{Initiated: true}, nil).Maybe()

	// Define test parameters
	op := pb.ReduceOp_SUM
	commId := uint64(1)

	// Set up device clients information
	deviceClientsInfo := map[uint64]*GPUDeviceClientInfo{
		0: {DeviceId: 0, Client: mockClient1, StreamIds: map[uint64][]uint64{commId: {}}},
		1: {DeviceId: 1, Client: mockClient2, StreamIds: map[uint64][]uint64{commId: {}}},
		2: {DeviceId: 2, Client: mockClient3, StreamIds: map[uint64][]uint64{commId: {}}},
	}

	// Define memory addresses for communication
	deviceMemAddrs := []*pb.DeviceMemAddr{
		{DeviceId: &pb.DeviceId{Value: 0}, SrcMemAddr: &pb.MemAddr{Value: 0}, DstMemAddr: &pb.MemAddr{Value: 1}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 1}, SrcMemAddr: &pb.MemAddr{Value: 8}, DstMemAddr: &pb.MemAddr{Value: 2}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 2}, SrcMemAddr: &pb.MemAddr{Value: 16}, DstMemAddr: &pb.MemAddr{Value: 3}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 0}, SrcMemAddr: &pb.MemAddr{Value: 24}, DstMemAddr: &pb.MemAddr{Value: 4}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 1}, SrcMemAddr: &pb.MemAddr{Value: 32}, DstMemAddr: &pb.MemAddr{Value: 5}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 2}, SrcMemAddr: &pb.MemAddr{Value: 40}, DstMemAddr: &pb.MemAddr{Value: 6}, NumBytes: 10},
	}

	// Create the service
	service := &GPUCoordinatorService{}

	// Call the method
	err := service.applyOpToRing(context.Background(), op, commId, deviceMemAddrs, deviceClientsInfo)

	// Assertions
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed begin send for rank 0 to 1")
	mockClient1.AssertExpectations(t)
	mockClient2.AssertExpectations(t)
}

func TestApplyOpToRing_BeginReceiveError(t *testing.T) {
	// Setup mock clients
	mockClient1 := new(gpuMock.MockGPUDeviceClient)
	mockClient2 := new(gpuMock.MockGPUDeviceClient)
	mockClient3 := new(gpuMock.MockGPUDeviceClient)

	// Simulate successful BeginSend
	mockClient1.On("BeginSend", mock.Anything, mock.Anything).Return(&pb.BeginSendResponse{Initiated: true, StreamId: &pb.StreamId{Value: 123}}, nil)

	// Simulate error in BeginReceive
	mockClient2.On("BeginReceive", mock.Anything, mock.Anything).Return(nil, status.Errorf(codes.Unavailable, "receive failed"))

	mockClient2.On("BeginSend", mock.Anything, mock.Anything).Return(&pb.BeginSendResponse{Initiated: true, StreamId: &pb.StreamId{Value: 123}}, nil).Maybe()
	mockClient2.On("BeginReceive", mock.Anything, mock.Anything).Return(&pb.BeginReceiveResponse{Initiated: true}, nil).Maybe()

	mockClient3.On("BeginSend", mock.Anything, mock.Anything).Return(&pb.BeginSendResponse{Initiated: true, StreamId: &pb.StreamId{Value: 123}}, nil).Maybe()
	mockClient3.On("BeginReceive", mock.Anything, mock.Anything).Return(&pb.BeginReceiveResponse{Initiated: true}, nil).Maybe()

	// Define test parameters
	op := pb.ReduceOp_SUM
	commId := uint64(1)

	// Set up device clients information
	deviceClientsInfo := map[uint64]*GPUDeviceClientInfo{
		0: {DeviceId: 0, Client: mockClient1, StreamIds: map[uint64][]uint64{commId: {}}},
		1: {DeviceId: 1, Client: mockClient2, StreamIds: map[uint64][]uint64{commId: {}}},
		2: {DeviceId: 2, Client: mockClient3, StreamIds: map[uint64][]uint64{commId: {}}},
	}

	// Define memory addresses for communication
	// 0 -> 1 -> 2 -> 0
	deviceMemAddrs := []*pb.DeviceMemAddr{
		{DeviceId: &pb.DeviceId{Value: 0}, SrcMemAddr: &pb.MemAddr{Value: 0}, DstMemAddr: &pb.MemAddr{Value: 1}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 1}, SrcMemAddr: &pb.MemAddr{Value: 8}, DstMemAddr: &pb.MemAddr{Value: 2}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 2}, SrcMemAddr: &pb.MemAddr{Value: 16}, DstMemAddr: &pb.MemAddr{Value: 3}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 0}, SrcMemAddr: &pb.MemAddr{Value: 24}, DstMemAddr: &pb.MemAddr{Value: 4}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 1}, SrcMemAddr: &pb.MemAddr{Value: 32}, DstMemAddr: &pb.MemAddr{Value: 5}, NumBytes: 10},
		{DeviceId: &pb.DeviceId{Value: 2}, SrcMemAddr: &pb.MemAddr{Value: 40}, DstMemAddr: &pb.MemAddr{Value: 6}, NumBytes: 10},
	}

	// Create the service
	service := &GPUCoordinatorService{}

	// Call the method
	err := service.applyOpToRing(context.Background(), op, commId, deviceMemAddrs, deviceClientsInfo)

	// Assertions
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed begin receive for rank 1 from 0")
	mockClient1.AssertExpectations(t)
	mockClient2.AssertExpectations(t)
}
