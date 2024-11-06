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

type shardCacheUpdate struct {
	shard int
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
	globalMu      sync.RWMutex
	shardCacheMap map[int]*shardCache
	nodeIndex     atomic.Uint32
}

func (server *KvServerImpl) handleShardMapUpdate() {
	shardArray := server.shardMap.ShardsForNode(server.nodeName)
	newShards := make(map[int]struct{}, len(shardArray))
	for _, shard := range shardArray {
		newShards[shard] = struct{}{}
	}

	server.globalMu.RLock()
	var add []int
	var remove []int
	for shard := range newShards {
		if _, ok := server.shardCacheMap[shard]; !ok {
			add = append(add, shard)
		}
	}
	for shard := range server.shardCacheMap {
		if _, ok := newShards[shard]; !ok {
			remove = append(remove, shard)
		}
	}
	server.globalMu.RUnlock()

	// update shards (includes remove and add)
	shardCacheUpdateCh := make(chan *shardCacheUpdate)
	doneUpdating := make(chan struct{})
	go func() {
		defer close(doneUpdating)
		var updates []*shardCacheUpdate
		for shardCacheUpdate := range shardCacheUpdateCh {
			updates = append(updates, shardCacheUpdate)
		}
		server.globalMu.Lock()
		for _, shard := range remove {
			delete(server.shardCacheMap, shard)
		}
		for _, update := range updates {
			server.shardCacheMap[update.shard] = &shardCache{cache: update.cache}
		}
		server.globalMu.Unlock()
	}()

	// add shards
	wg := &sync.WaitGroup{}
	for _, shard := range add {
		wg.Add(1)
		func(shard int, updateCh chan<- *shardCacheUpdate) {
			defer wg.Done()
			server.sendShardCacheUpdates(shard, updateCh)
		}(shard, shardCacheUpdateCh)
	}
	wg.Wait()
	close(shardCacheUpdateCh)
	<-doneUpdating
}

func (server *KvServerImpl) sendShardCacheUpdates(shard int, updateCh chan<- *shardCacheUpdate) {
	nodes := server.shardMap.NodesForShard(shard)
	n_nodes := len(nodes)
	var kvClient proto.KvClient
	var response *proto.GetShardContentsResponse
	var err error
	for untriedNodes := n_nodes; untriedNodes > 0; untriedNodes-- {
		nodeIndex := int(server.nodeIndex.Add(1)) % n_nodes
		nodeName := nodes[nodeIndex]
		if nodeName == server.nodeName {
			continue
		}
		if kvClient, err = server.clientPool.GetClient(nodeName); err != nil {
			continue
		}
		response, err = kvClient.GetShardContents(context.Background(), &proto.GetShardContentsRequest{Shard: int32(shard)})
		if err != nil {
			continue
		}
		shardValues := response.GetValues()
		if shardValues == nil {
			continue
		}
		cache := make(map[string]cacheEntry)
		for _, shardValue := range shardValues {
			cache[shardValue.GetKey()] = cacheEntry{
				value:  shardValue.GetValue(),
				expiry: time.Now().Add(time.Duration(shardValue.GetTtlMsRemaining()) * time.Millisecond),
			}
		}
		updateCh <- &shardCacheUpdate{shard: shard, cache: cache}
		return

	}
	updateCh <- &shardCacheUpdate{shard: shard, cache: make(map[string]cacheEntry)}
	logrus.Errorf("could not get contents of shard %v", shard)
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
	listener := shardMap.MakeListener()
	server := KvServerImpl{
		nodeName:   nodeName,
		shardMap:   shardMap,
		listener:   &listener,
		clientPool: clientPool,
		shutdown:   make(chan struct{}),

		numShards:     shardMap.NumShards(),
		shardCacheMap: make(map[int]*shardCache),
		globalMu:      sync.RWMutex{},
		nodeIndex:     atomic.Uint32{},
	}
	server.handleShardMapUpdate()
	go server.shardMapListenLoop()
	go server.startExpirationCleanup(800 * time.Millisecond)
	return &server
}

func (server *KvServerImpl) Shutdown() {
	close(server.shutdown)
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

	server.globalMu.RLock()
	shardCache, ok := server.shardCacheMap[shard]
	server.globalMu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "server does not host corresponding shard")
	}

	shardCache.mu.RLock()
	cacheEntry, ok := shardCache.cache[key]
	shardCache.mu.RUnlock()
	if ok {
		if cacheEntry.IsExpired() {
			return &proto.GetResponse{Value: "", WasFound: false}, nil
		}
		return &proto.GetResponse{Value: cacheEntry.value, WasFound: ok}, nil
	}
	return &proto.GetResponse{Value: "", WasFound: ok}, nil
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
	server.globalMu.RLock()
	shardCache, ok := server.shardCacheMap[shard]
	server.globalMu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "server does not host corresponding shard")
	}

	value := request.GetValue()
	ttl := time.Duration(request.GetTtlMs()) * time.Millisecond
	shardCache.mu.Lock()
	shardCache.cache[key] = cacheEntry{value: value, expiry: time.Now().Add(ttl)}
	shardCache.mu.Unlock()
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
	server.globalMu.RLock()
	shardCache, ok := server.shardCacheMap[shard]
	server.globalMu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "server does not host corresponding shard")
	}
	shardCache.mu.Lock()
	delete(shardCache.cache, key)
	shardCache.mu.Unlock()
	return &proto.DeleteResponse{}, nil
}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "shard": request.Shard},
	).Trace("node received GetShardContents() request")

	shard := int(request.GetShard())
	server.globalMu.RLock()
	shardCache, ok := server.shardCacheMap[shard]
	server.globalMu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "server does not host corresponding shard")
	}
	shardCache.mu.RLock()
	values := []*proto.GetShardValue{}
	for key, cache := range shardCache.cache {
		values = append(
			values,
			&proto.GetShardValue{
				Key:            key,
				Value:          cache.value,
				TtlMsRemaining: time.Until(cache.expiry).Milliseconds(),
			},
		)
	}
	shardCache.mu.RUnlock()
	return &proto.GetShardContentsResponse{Values: values}, nil
}

func (server *KvServerImpl) startExpirationCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-server.shutdown:
			return
		case <-ticker.C:
			shardCaches := []*shardCache{}

			server.globalMu.RLock()
			for _, serverShardCaches := range server.shardCacheMap {
				shardCaches = append(shardCaches, serverShardCaches)
			}
			server.globalMu.RUnlock()

			for _, shardCache := range shardCaches {
				shardCache.mu.Lock()
				for key, entry := range shardCache.cache {
					if entry.IsExpired() {
						delete(shardCache.cache, key)
					}
				}
				shardCache.mu.Unlock()
			}
		}
	}
}
