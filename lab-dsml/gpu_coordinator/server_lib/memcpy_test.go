package server_lib

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	gpu "cs426.yale.edu/lab-dsml/gpu_device/server_lib"
	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
)

func TestMemcpy_DeviceToHost_Success(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "test_memcpy_*.bin")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name()) // Clean up the file after the test

	// Write some content to the file
	content := []byte("Hello, world!")
	_, err = tmpFile.Write(content)
	assert.Nil(t, err)
	tmpFile.Close() // Close file to allow reading in the service

	deviceId := uint64(0)
	// Set up the device config with the temporary file path
	coordinator := &GPUCoordinatorService{
		config: map[uint64]*gpu.GPUDeviceConfig{
			deviceId: {MemoryFilePath: tmpFile.Name()},
		},
	}

	// Define test input
	req := &pb.MemcpyRequest{
		Either: &pb.MemcpyRequest_DeviceToHost{
			DeviceToHost: &pb.MemcpyDeviceToHostRequest{
				SrcDeviceId: &pb.DeviceId{Value: deviceId},
				SrcMemAddr:  &pb.MemAddr{Value: 0},
				NumBytes:    13,
			},
		},
	}

	// Call the function
	resp, err := coordinator.Memcpy(context.Background(), req)

	// Assertions
	assert.Nil(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, len(resp.GetDeviceToHost().DstData), 13)

	// Verify the content of the destination data
	assert.Equal(t, resp.GetDeviceToHost().DstData, content)

	// Define new test input
	req = &pb.MemcpyRequest{
		Either: &pb.MemcpyRequest_DeviceToHost{
			DeviceToHost: &pb.MemcpyDeviceToHostRequest{
				SrcDeviceId: &pb.DeviceId{Value: deviceId},
				SrcMemAddr:  &pb.MemAddr{Value: 2},
				NumBytes:    5,
			},
		},
	}

	// Call the function
	resp, err = coordinator.Memcpy(context.Background(), req)

	// Assertions
	assert.Nil(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, len(resp.GetDeviceToHost().DstData), 5)

	// Verify the content of the destination data
	assert.Equal(t, resp.GetDeviceToHost().DstData, content[2:7])
}

func TestMemcpy_DeviceToHost_DeviceNotFound(t *testing.T) {
	service := &GPUCoordinatorService{
		config: make(map[uint64]*gpu.GPUDeviceConfig),
	}

	req := &pb.MemcpyRequest{
		Either: &pb.MemcpyRequest_DeviceToHost{
			DeviceToHost: &pb.MemcpyDeviceToHostRequest{
				SrcDeviceId: &pb.DeviceId{Value: 0},
				SrcMemAddr:  &pb.MemAddr{Value: 0},
				NumBytes:    10,
			},
		},
	}

	resp, err := service.Memcpy(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no device 0 in config")
}

func TestMemcpy_DeviceToHost_FileOpenError(t *testing.T) {
	// Set up the service with an invalid path
	coordinator := &GPUCoordinatorService{
		config: map[uint64]*gpu.GPUDeviceConfig{
			0: {MemoryFilePath: "/fake/path"},
		},
	}

	req := &pb.MemcpyRequest{
		Either: &pb.MemcpyRequest_DeviceToHost{
			DeviceToHost: &pb.MemcpyDeviceToHostRequest{
				SrcDeviceId: &pb.DeviceId{Value: 0},
				SrcMemAddr:  &pb.MemAddr{Value: 0},
				NumBytes:    10,
			},
		},
	}

	resp, err := coordinator.Memcpy(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to open file /fake/path")
}

func TestMemcpy_HostToDevice_Success(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "test_memcpy_*.bin")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name()) // Clean up the file after the test

	// Set up the device config with the temporary file path
	coordinator := &GPUCoordinatorService{
		config: map[uint64]*gpu.GPUDeviceConfig{
			0: {MemoryFilePath: tmpFile.Name()},
		},
	}

	// Define test input
	req := &pb.MemcpyRequest{
		Either: &pb.MemcpyRequest_HostToDevice{
			HostToDevice: &pb.MemcpyHostToDeviceRequest{
				DstDeviceId: &pb.DeviceId{Value: 0},
				DstMemAddr:  &pb.MemAddr{Value: 0},
				HostSrcData: []byte{1, 2, 3},
			},
		},
	}

	// Call the function
	resp, err := coordinator.Memcpy(context.Background(), req)

	// Assertions
	assert.Nil(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.GetHostToDevice().Success)

	// Verify that data was written to the temporary file
	// Reopen the file to check the content
	tmpFile, err = os.Open(tmpFile.Name())
	assert.Nil(t, err)
	defer tmpFile.Close()

	fileContent := make([]byte, 3)
	_, err = tmpFile.Read(fileContent)
	assert.Nil(t, err)
	assert.Equal(t, fileContent, []byte{1, 2, 3})

}

func TestMemcpy_HostToDevice_DeviceNotFound(t *testing.T) {
	service := &GPUCoordinatorService{
		config: make(map[uint64]*gpu.GPUDeviceConfig),
	}

	req := &pb.MemcpyRequest{
		Either: &pb.MemcpyRequest_HostToDevice{
			HostToDevice: &pb.MemcpyHostToDeviceRequest{
				DstDeviceId: &pb.DeviceId{Value: 0},
				DstMemAddr:  &pb.MemAddr{Value: 0},
				HostSrcData: []byte{1, 2, 3},
			},
		},
	}

	resp, err := service.Memcpy(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no device 0 in config")
}
