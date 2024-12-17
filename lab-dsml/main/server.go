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
	gpu "cs426.yale.edu/lab-dsml/gpu_device/server_lib" // Import path for the GPU device service
	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"          // Import path for the protobuf definitions
	"google.golang.org/grpc"
)

// Config represents the entire configuration for all devices and the coordinator
type Config struct {
	Devices            map[uint64]*gpu.GPUDeviceConfig `json:"devices"`
	CoordinatorAddress string                          `json:"coordinator_address"`
}

// Function to allocate 1MB of space to the GPU memory file
func allocateFileSpace(filepath string, size int64) error {
	// Get the current working directory
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("error getting current working directory: %w", err)
	}

	// If the filepath is relative, make it absolute by prepending the cwd
	if !path.IsAbs(filepath) {
		filepath = path.Join(cwd, filepath)
	}

	// Create the file or overwrite it if it already exists
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("error creating file %s: %w", filepath, err)
	}
	defer file.Close()

	// Extend or truncate the file to the required size
	err = file.Truncate(size)
	if err != nil {
		return fmt.Errorf("error allocating space for file %s: %w", filepath, err)
	}

	// write zeros to initialize the file content
	_, err = file.Write(make([]byte, size))
	if err != nil {
		return fmt.Errorf("error writing initial content to file %s: %w", filepath, err)
	}

	return nil
}

// Function to launch GPU Device Server using the configuration
func launchGPUDeviceServer(deviceId uint64, config map[uint64]*gpu.GPUDeviceConfig) {
	// Allocate 1MB of space for the GPU memory file
	requiredSize := int64(1024 * 1024)
	err := allocateFileSpace(config[deviceId].MemoryFilePath, requiredSize)
	if err != nil {
		log.Fatalf("failed to allocate space for memory file %s: %v", config[deviceId].MemoryFilePath, err)
	}

	// Listen on the specified address (e.g., [::1]:8081)
	listen, err := net.Listen("tcp", config[deviceId].Address)
	if err != nil {
		log.Fatalf("failed to listen on address %s: %v", config[deviceId].Address, err)
	}

	// Create a new gRPC server
	s := grpc.NewServer()

	// Initialize GPU device service using NewGPUDeviceService
	gpuService := gpu.NewGPUDeviceService(config, deviceId, 0, uint64(requiredSize))

	// Register the GPU device server for the given device config
	pb.RegisterGPUDeviceServer(s, gpuService)

	// Start serving on the specified address
	log.Printf("GPU Device Server %d listening on %s", deviceId, config[deviceId].Address)
	if err := s.Serve(listen); err != nil {
		log.Fatalf("failed to serve gRPC server on %s: %v", config[deviceId].Address, err)
	}
}

// launchGPUCoordinatorServer initializes and launches the GPU coordinator service
func launchGPUCoordinatorServer(config map[uint64]*gpu.GPUDeviceConfig, coordinatorAddress string) {
	listen, err := net.Listen("tcp", coordinatorAddress)
	if err != nil {
		log.Fatalf("failed to listen on coordinator address %s: %v", coordinatorAddress, err)
	}

	// Create a new gRPC server for the coordinator
	s := grpc.NewServer()

	// Initialize the GPU coordinator service
	gpuCoordinatorService := gc.NewGPUCoordinatorService(config)

	// Register the GPU coordinator server
	pb.RegisterGPUCoordinatorServer(s, gpuCoordinatorService)

	// Start serving on the specified address
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
	// Load the configuration from the JSON file
	config, err := loadConfig("config/gpu_config.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Launch GPU device servers based on the configuration
	for deviceId := range config.Devices {
		go launchGPUDeviceServer(deviceId, config.Devices)
	}

	// Launch the GPU coordinator server
	go launchGPUCoordinatorServer(config.Devices, config.CoordinatorAddress)

	// Block main goroutine to keep the servers running
	select {}
}
