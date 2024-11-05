package kv

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool

	// Add any client-side state you want here
	numShards int
	nodeIndex atomic.Uint32
}

func MakeKv(shardMap *ShardMap, clientPool ClientPool) *Kv {
	kv := &Kv{
		shardMap:   shardMap,
		clientPool: clientPool,

		numShards: shardMap.NumShards(),
		nodeIndex: atomic.Uint32{},
	}
	// Add any initialization logic
	return kv
}

func (kv *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	shard := GetShardForKey(key, kv.numShards)
	nodes := kv.shardMap.NodesForShard(shard)
	n_nodes := len(nodes)
	if n_nodes == 0 {
		return "", false, status.Error(codes.NotFound, "No nodes host shard")
	}

	var kvClient proto.KvClient
	var response *proto.GetResponse
	var err error
	for untriedNodes := n_nodes; untriedNodes > 0; untriedNodes-- {
		nodeIndex := int(kv.nodeIndex.Add(1)) % n_nodes
		kvClient, err = kv.clientPool.GetClient(nodes[nodeIndex])
		if err != nil {
			continue
		}
		response, err = kvClient.Get(ctx, &proto.GetRequest{Key: key})
		if err == nil {
			return response.GetValue(), response.GetWasFound(), err
		}
	}
	return "", false, err
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	shard := GetShardForKey(key, kv.numShards)
	nodes := kv.shardMap.NodesForShard(shard)
	n_nodes := len(nodes)
	if n_nodes == 0 {
		return status.Error(codes.NotFound, "No nodes host shard")
	}

	wg := &sync.WaitGroup{}
	errCh := make(chan error)
	errs := []error{}
	errDoneCh := make(chan struct{})
	go func() {
		defer close(errDoneCh)
		for err := range errCh {
			errs = append(errs, err)
		}
	}()
	for _, node := range nodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			kvClient, err := kv.clientPool.GetClient(node)
			if err != nil {
				errCh <- err
				return
			}
			_, err = kvClient.Set(ctx, &proto.SetRequest{Key: key, Value: value, TtlMs: ttl.Milliseconds()})
			if err != nil {
				errCh <- err
			}
		}(node)
	}
	wg.Wait()
	close(errCh)
	<-errDoneCh
	if len(errs) > 0 {
		return status.Errorf(codes.Unknown, "errors while kv.Set(): %v", errs)
	}
	return nil
}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	shard := GetShardForKey(key, kv.numShards)
	nodes := kv.shardMap.NodesForShard(shard)
	n_nodes := len(nodes)
	if n_nodes == 0 {
		return status.Error(codes.NotFound, "No nodes host shard")
	}

	wg := &sync.WaitGroup{}
	errCh := make(chan error)
	errs := []error{}
	errDoneCh := make(chan struct{})
	go func() {
		defer close(errDoneCh)
		for err := range errCh {
			errs = append(errs, err)
		}
	}()
	for _, node := range nodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			kvClient, err := kv.clientPool.GetClient(node)
			if err != nil {
				errCh <- err
				return
			}
			_, err = kvClient.Delete(ctx, &proto.DeleteRequest{Key: key})
			if err != nil {
				errCh <- err
			}
		}(node)
	}
	wg.Wait()
	close(errCh)
	<-errDoneCh
	if len(errs) > 0 {
		return status.Errorf(codes.Unknown, "errors while kv.Delete(): %v", errs)
	}
	return nil
}
