package server_lib

import (
	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
)

type Memory struct {
	Data          *[]byte
	InputAddr     uint64
	InputSize     uint64
	WeightAddr    uint64
	WeightSize    uint64
	OutputAddr    uint64
	OutputSize    uint64
	GradientAddrs []uint64
	GradientSizes []uint64
}

// On AllReduceRing, need to make n * (n - 1) rounds twice (once for reduce then once for gather)
// e.g. for 4 devices, with the notation being (rank[rcvId, sendId] ... => rank[id0 id1 id2 id3]):
// REDUCE
// phase1: 0[3,0] 1[0,1] 2[1,2] 3[2,3] => 0[0 1 2 33] 1[00 1 2 3] 2[0 11 2 3] 3[0 1 22 3]
// phase2: 0[2,3] 1[3,0] 2[0,1] 3[1,2] => 0[0 1 222 33] 1[00 1 2 333] 2[000 11 2 3] 3[0 111 22 3]
// phase3: 0[1,2] 1[2,3] 2[3,0] 3[0,1] => 0[0 1111 222 33] 1[00 1 2222 333] 2[000 11 2 3333] 3[0000 111 22 3]
// GATHER
// phase1: 0[0,1] 1[1,2] 2[2,3] 3[3,0] => 0[0000 1111 222 33] 1[00 1111 2222 333] 2[000 11 2222 3333] 3[0000 111 22 3333]
// phase2: 0[3,0] 1[0,1] 2[1,2] 3[2,3] => 0[0000 1111 222 3333] 1[0000 1111 2222 333] 2[000 1111 2222 3333] 3[0000 111 2222 3333]
// phase3: 0[2,3] 1[3,0] 2[0,1] 3[1,2] => 0[0000 1111 2222 3333] 1[0000 1111 2222 3333] 2[0000 1111 2222 3333] 3[0000 1111 2222 3333]
func ComputeDeviceMemAddrsForAllReduceRing(
	deviceIds []uint64,
	deviceMemMap map[uint64]*Memory,
	numDevices uint32,
) ([]*pb.DeviceMemAddr, []*pb.DeviceMemAddr) {
	var reduceDeviceMemAddrs []*pb.DeviceMemAddr
	var gatherDeviceMemAddrs []*pb.DeviceMemAddr
	for offset := range numDevices - 1 {
		// id of gradient chunk to send
		i := (numDevices - offset) % numDevices
		for _, srcId := range deviceIds {
			dstId := (srcId + 1) % uint64(len(deviceIds))
			srcMemory := deviceMemMap[srcId]
			dstMemory := deviceMemMap[dstId]
			reduceDeviceMemAddrs = append(reduceDeviceMemAddrs, &pb.DeviceMemAddr{
				DeviceId:   &pb.DeviceId{Value: srcId},
				SrcMemAddr: &pb.MemAddr{Value: srcMemory.GradientAddrs[i]},
				DstMemAddr: &pb.MemAddr{Value: dstMemory.GradientAddrs[i]},
				NumBytes:   srcMemory.GradientSizes[i],
			})
			i = (i + 1) % numDevices
		}
	}
	for offset := range numDevices - 1 {
		i := (numDevices - offset - 1) % numDevices
		for _, srcId := range deviceIds {
			dstId := (srcId + 1) % uint64(len(deviceIds))
			srcMemory := deviceMemMap[srcId]
			dstMemory := deviceMemMap[dstId]
			gatherDeviceMemAddrs = append(gatherDeviceMemAddrs, &pb.DeviceMemAddr{
				DeviceId:   &pb.DeviceId{Value: srcId},
				SrcMemAddr: &pb.MemAddr{Value: srcMemory.GradientAddrs[i]},
				DstMemAddr: &pb.MemAddr{Value: dstMemory.GradientAddrs[i]},
				NumBytes:   srcMemory.GradientSizes[i],
			})
			i = (i + 1) % numDevices
		}
	}
	return reduceDeviceMemAddrs, gatherDeviceMemAddrs
}
