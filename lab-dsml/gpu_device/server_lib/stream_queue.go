package server_lib

import (
	"sync"

	pb "cs426.yale.edu/lab-dsml/gpu_sim/proto"
)

type StreamInfo struct {
	StreamId uint64
	Op       pb.ReduceOp
	MemAddr  uint64
	NumBytes uint64
	SrcId    uint64
	DstId    uint64
}

type ConcurrentStreamInfoQueueImpl struct {
	queue []*StreamInfo
	mu    sync.RWMutex
}

type ConcurrentStreamInfoQueue interface {
	Peek() *StreamInfo
	Enqueue(streamInfo *StreamInfo)
	Dequeue() *StreamInfo
	Empty() bool
	Size() int
}

func NewConcurrentStreamInfoQueue() ConcurrentStreamInfoQueue {
	return &ConcurrentStreamInfoQueueImpl{}
}

func (q *ConcurrentStreamInfoQueueImpl) Empty() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.queue) == 0
}

func (q *ConcurrentStreamInfoQueueImpl) Peek() *StreamInfo {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if len(q.queue) > 0 {
		return q.queue[0]
	}
	return nil
}

func (q *ConcurrentStreamInfoQueueImpl) Enqueue(streamInfo *StreamInfo) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, streamInfo)
}

func (q *ConcurrentStreamInfoQueueImpl) Dequeue() *StreamInfo {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) > 0 {
		val := q.queue[0]
		q.queue = q.queue[1:]
		return val
	}
	return nil
}

func (q *ConcurrentStreamInfoQueueImpl) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.queue)
}
