package kv

import (
	"context"
	"sync"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cacheEntry struct {
	value  string
	expiry time.Time
}

func (ce cacheEntry) IsExpired() bool {
	return time.Now().After(ce.expiry)
}

type shardCache struct {
	mu    sync.RWMutex
	cache map[string]cacheEntry
}

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	clientPool ClientPool
	shutdown   chan struct{}

	numShards     int
	shardSet      *IntSet
	shardCacheMap map[int]*shardCache
}

func (server *KvServerImpl) handleShardMapUpdate() {
	// TODO: Part C
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			return
		case <-listener:
			server.handleShardMapUpdate()
		}
	}
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	numShards := shardMap.NumShards()
	shardArray := shardMap.ShardsForNode(nodeName)
	shardSet := NewSet()
	for _, shard := range shardArray {
		shardSet.Add(shard)
	}

	shardCacheMap := make(map[int]*shardCache)
	// shards are 1-indexed
	for i := range numShards {
		shardCacheMap[i+1] = &shardCache{cache: make(map[string]cacheEntry)}
	}

	listener := shardMap.MakeListener()
	server := KvServerImpl{
		nodeName:   nodeName,
		shardMap:   shardMap,
		listener:   &listener,
		clientPool: clientPool,
		shutdown:   make(chan struct{}),

		numShards:     numShards,
		shardSet:      shardSet,
		shardCacheMap: shardCacheMap,
	}
	go server.shardMapListenLoop()
	server.handleShardMapUpdate()
	go server.startExpirationCleanup(800 * time.Millisecond)
	return &server
}

func (server *KvServerImpl) Shutdown() {
	server.shutdown <- struct{}{}
	server.listener.Close()
}

func (server *KvServerImpl) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	// Trace-level logging for node receiving this request (enable by running with -log-level=trace),
	// feel free to use Trace() or Debug() logging in your code to help debug tests later without
	// cluttering logs by default. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Get() request")

	key := request.GetKey()
	if key == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Get() received empty key")
	}
	shard := GetShardForKey(key, server.numShards)
	if !server.shardSet.Contains(shard) {
		return nil, status.Error(codes.NotFound, "server does not host corresponding shard")
	}

	shardCache := server.shardCacheMap[shard]
	shardCache.mu.RLock()
	defer shardCache.mu.RUnlock()
	value := ""
	cacheEntry, ok := shardCache.cache[key]
	if ok {
		if cacheEntry.IsExpired() {
			return &proto.GetResponse{Value: "", WasFound: false}, nil
		}
		value = cacheEntry.value
	}
	return &proto.GetResponse{Value: value, WasFound: ok}, nil
}

func (server *KvServerImpl) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Set() request")

	key := request.GetKey()
	if key == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Set() received empty key")
	}
	shard := GetShardForKey(key, server.numShards)
	if !server.shardSet.Contains(shard) {
		return nil, status.Error(codes.NotFound, "server does not host corresponding shard")
	}

	value := request.GetValue()
	ttl := time.Duration(request.GetTtlMs()) * time.Millisecond
	shardCache := server.shardCacheMap[shard]
	shardCache.mu.Lock()
	defer shardCache.mu.Unlock()
	shardCache.cache[key] = cacheEntry{value: value, expiry: time.Now().Add(ttl)}
	return &proto.SetResponse{}, nil
}

func (server *KvServerImpl) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Delete() request")

	key := request.GetKey()
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "Delete() received empty key")
	}
	shard := GetShardForKey(key, server.numShards)
	if !server.shardSet.Contains(shard) {
		return nil, status.Error(codes.NotFound, "server does not host corresponding shard")
	}

	shardCache := server.shardCacheMap[shard]
	shardCache.mu.Lock()
	defer shardCache.mu.Unlock()
	delete(shardCache.cache, key)
	return &proto.DeleteResponse{}, nil
}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	panic("TODO: Part C")
}

func (server *KvServerImpl) startExpirationCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for shard, shardCache := range server.shardCacheMap {
				if !server.shardSet.Contains(shard) {
					continue
				}
				shardCache.mu.Lock()
				for key, entry := range shardCache.cache {
					if entry.IsExpired() {
						delete(shardCache.cache, key)
					}
				}
				shardCache.mu.Unlock()
			}
		case <-server.shutdown:
			return
		}
	}
}
