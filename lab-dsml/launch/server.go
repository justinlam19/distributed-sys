package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"sync"

	gc "cs426.yale.edu/lab-dsml/gpu_coordinator/server_lib"
	gpu "cs426.yale.edu/lab-dsml/gpu_device/server_lib"
	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
	"google.golang.org/grpc"
)

// entire configuration for all devices and the coordinator
type Config struct {
	Devices            map[uint64]*gpu.GPUDeviceConfig `json:"devices"`
	CoordinatorAddress string                          `json:"coordinator_address"`
}

func allocateFileSpace(filepath string, size int64) error {
	// Get the current working directory
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("error getting current working directory: %w", err)
	}

	// If the filepath is relative, make it absolute by prepending the working directory
	if !path.IsAbs(filepath) {
		filepath = path.Join(cwd, filepath)
	}

	// create the file or overwrite it if it already exists
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("error creating file %s: %w", filepath, err)
	}
	defer file.Close()

	err = file.Truncate(size)
	if err != nil {
		return fmt.Errorf("error allocating space for file %s: %w", filepath, err)
	}

	// write zeros to the file
	_, err = file.Write(make([]byte, size))
	if err != nil {
		return fmt.Errorf("error writing initial content to file %s: %w", filepath, err)
	}

	return nil
}

func launchGPUDeviceServer(deviceId uint64, config map[uint64]*gpu.GPUDeviceConfig) {
	// Allocate 1MB of space for the GPU memory file
	requiredSize := int64(1024 * 1024)
	err := allocateFileSpace(config[deviceId].MemoryFilePath, requiredSize)
	if err != nil {
		log.Fatalf("failed to allocate space for memory file %s: %v", config[deviceId].MemoryFilePath, err)
	}
	listen, err := net.Listen("tcp", config[deviceId].Address)
	if err != nil {
		log.Fatalf("failed to listen on address %s: %v", config[deviceId].Address, err)
	}
	s := grpc.NewServer()
	gpuService := gpu.NewGPUDeviceService(config, deviceId, 0, uint64(requiredSize))
	pb.RegisterGPUDeviceServer(s, gpuService)
	log.Printf("GPU Device Server %d listening on %s", deviceId, config[deviceId].Address)
	if err := s.Serve(listen); err != nil {
		log.Fatalf("failed to serve gRPC server on %s: %v", config[deviceId].Address, err)
	}
}

func launchGPUCoordinatorServer(config map[uint64]*gpu.GPUDeviceConfig, coordinatorAddress string) {
	listen, err := net.Listen("tcp", coordinatorAddress)
	if err != nil {
		log.Fatalf("failed to listen on coordinator address %s: %v", coordinatorAddress, err)
	}
	s := grpc.NewServer()
	gpuCoordinatorService := gc.NewGPUCoordinatorService(config)
	pb.RegisterGPUCoordinatorServer(s, gpuCoordinatorService)
	log.Printf("GPU Coordinator Server listening on %s", coordinatorAddress)
	if err := s.Serve(listen); err != nil {
		log.Fatalf("failed to serve gRPC server on %s: %v", coordinatorAddress, err)
	}
}

func loadConfig(filePath string) (*Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening config file: %w", err)
	}
	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		return nil, fmt.Errorf("error decoding config file: %w", err)
	}

	// Initialize the RWMutex for each device
	for _, deviceConfig := range config.Devices {
		deviceConfig.MemoryLock = &sync.RWMutex{}
	}

	return &config, nil
}

func main() {
	// hard coded gpu config path
	config, err := loadConfig("config/gpu_config.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	for deviceId := range config.Devices {
		go launchGPUDeviceServer(deviceId, config.Devices)
	}
	go launchGPUCoordinatorServer(config.Devices, config.CoordinatorAddress)
	// block main goroutine to keep the servers running
	select {}
}
