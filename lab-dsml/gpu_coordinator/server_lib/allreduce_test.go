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
	).Times(2)
	mockClient1.On("BeginReceive", mock.Anything, mock.Anything).Return(
		&pb.BeginReceiveResponse{Initiated: true},
		nil,
	).Times(2)

	mockClient2.On("BeginSend", mock.Anything, mock.Anything).Return(
		&pb.BeginSendResponse{Initiated: true, StreamId: &pb.StreamId{Value: 123}},
		nil,
	).Times(2)
	mockClient2.On("BeginReceive", mock.Anything, mock.Anything).Return(
		&pb.BeginReceiveResponse{Initiated: true},
		nil,
	).Times(2)

	mockClient3.On("BeginSend", mock.Anything, mock.Anything).Return(
		&pb.BeginSendResponse{Initiated: true, StreamId: &pb.StreamId{Value: 123}},
		nil,
	).Times(2)
	mockClient3.On("BeginReceive", mock.Anything, mock.Anything).Return(
		&pb.BeginReceiveResponse{Initiated: true},
		nil,
	).Times(2)

	// Set up device clients information
	deviceClientsInfo := []*GPUDeviceClientInfo{
		{DeviceId: 1, Client: mockClient1, StreamIds: make(map[uint64][]uint64)},
		{DeviceId: 2, Client: mockClient2, StreamIds: make(map[uint64][]uint64)},
		{DeviceId: 3, Client: mockClient3, StreamIds: make(map[uint64][]uint64)},
	}

	// Define memory addresses for communication
	// 0 -> 1 -> 2 -> 3
	// 3 -> 4 -> 5 -> 0
	memAddrs := map[uint32]*pb.MemAddr{
		0: {Value: 0},  // 0->1 using address 0 for both
		1: {Value: 8},  // 1->2 using address 8 for both
		2: {Value: 16}, // 2->3 using address 16 for both
		3: {Value: 24},
		4: {Value: 32},
		5: {Value: 40},
	}

	// Create the service
	service := &GPUCoordinatorService{}

	// Define test parameters
	op := pb.ReduceOp_SUM
	commId := uint64(1)
	numBytes := uint64(10)
	numDevices := uint32(3)

	// Call the method
	err := service.applyOpToRing(context.Background(), op, commId, numBytes, numDevices, memAddrs, deviceClientsInfo)

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

	// Set up device clients information
	deviceClientsInfo := []*GPUDeviceClientInfo{
		{DeviceId: 1, Client: mockClient1, StreamIds: make(map[uint64][]uint64)},
		{DeviceId: 2, Client: mockClient2, StreamIds: make(map[uint64][]uint64)},
		{DeviceId: 3, Client: mockClient3, StreamIds: make(map[uint64][]uint64)},
	}

	// Define memory addresses for communication
	memAddrs := map[uint32]*pb.MemAddr{
		0: {Value: 0},
		1: {Value: 100},
		2: {Value: 9},
		3: {Value: 5},
		4: {Value: 105},
		5: {Value: 14},
	}

	// Create the service
	service := &GPUCoordinatorService{}

	// Define test parameters
	op := pb.ReduceOp_SUM
	commId := uint64(1)
	numBytes := uint64(10)
	numDevices := uint32(3)

	// Call the method
	err := service.applyOpToRing(context.Background(), op, commId, numBytes, numDevices, memAddrs, deviceClientsInfo)

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

	// Set up device clients information
	deviceClientsInfo := []*GPUDeviceClientInfo{
		{DeviceId: 1, Client: mockClient1, StreamIds: make(map[uint64][]uint64)},
		{DeviceId: 2, Client: mockClient2, StreamIds: make(map[uint64][]uint64)},
		{DeviceId: 3, Client: mockClient3, StreamIds: make(map[uint64][]uint64)},
	}

	// Define memory addresses for communication
	memAddrs := map[uint32]*pb.MemAddr{
		0: {Value: 0},
		1: {Value: 100},
		2: {Value: 9},
		3: {Value: 5},
		4: {Value: 105},
		5: {Value: 14},
	}

	// Create the service
	service := &GPUCoordinatorService{}

	// Define test parameters
	op := pb.ReduceOp_SUM
	commId := uint64(1)
	numBytes := uint64(10)
	numDevices := uint32(3)

	// Call the method
	err := service.applyOpToRing(context.Background(), op, commId, numBytes, numDevices, memAddrs, deviceClientsInfo)

	// Assertions
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed begin receive for rank 1 from 0")
	mockClient1.AssertExpectations(t)
	mockClient2.AssertExpectations(t)
}
