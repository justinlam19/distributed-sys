package server_lib

import (
	"context"
	"testing"

	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
	"github.com/stretchr/testify/assert"
)

func TestBeginSend(t *testing.T) {
	// Create a mock GPUDeviceService
	config := map[uint64]*GPUDeviceConfig{
		1: {Address: "localhost:5001", MemoryFilePath: "/tmp/device1.mem"},
		2: {Address: "localhost:5002", MemoryFilePath: "/tmp/device2.mem"},
	}
	service := NewGPUDeviceService(config, 1, 0, 1000)

	// Prepare the request
	req := &pb.BeginSendRequest{
		DstDeviceId: &pb.DeviceId{Value: 2},
		NumBytes:    100,
	}

	// Call the BeginSend method
	resp, err := service.BeginSend(context.Background(), req)

	// Assertions
	assert.NoError(t, err)
	assert.True(t, resp.Initiated)
	assert.NotNil(t, resp.StreamId)
	assert.Equal(t, pb.Status_IN_PROGRESS, service.StreamStatuses[resp.StreamId.Value])
}
