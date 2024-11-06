package kvtest

import (
	"context"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cs426.yale.edu/lab4/kv"
	"cs426.yale.edu/lab4/kv/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Like previous labs, you must write some tests on your own.
// Add your test cases in this file and submit them as extra_test.go.
// You must add at least 5 test cases, though you can add as many as you like.
//
// You can use any type of test already used in this lab: server
// tests, client tests, or integration tests.
//
// You can also write unit tests of any utility functions you have in utils.go
//
// Tests are run from an external package, so you are testing the public API
// only. You can make methods public (e.g. utils) by making them Capitalized.

func TestExtraNoLeakedGoroutinesAfterConcurrentGetsAndSets(t *testing.T) {
	// Even after many concurrent gets and sets, after setup.shutdown(), goroutines should exit
	// Check if the final number of goroutines is the same as the initial number
	// New note: this was actually able to catch a leaked goroutine (more in jl4268.time.log)
	initialNumGoroutine := runtime.NumGoroutine()

	setup := MakeTestSetup(MakeFourNodesWithFiveShards())

	const numGoros = 15
	const numIters = 150
	keys := RandomKeys(100, 20)
	vals := RandomKeys(100, 40)

	found := make([]int32, 200)
	var wg sync.WaitGroup
	wg.Add(numGoros)
	for i := 0; i < numGoros; i++ {
		go func(i int) {
			for j := 0; j < numIters; j++ {
				err := setup.Set(keys[(i*100+j)%79], vals[(j*100+i)%79], 100*time.Second)
				assert.Nil(t, err)
				for k := 0; k < 100; k++ {
					_, wasFound, err := setup.Get(keys[k])
					assert.Nil(t, err)
					if wasFound {
						atomic.StoreInt32(&found[k], 1)
					}
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	for i := 0; i < 79; i++ {
		assert.True(t, found[i] == 1)
	}
	for i := 79; i < 100; i++ {
		assert.False(t, found[i] == 1)
	}

	setup.Shutdown()
	time.Sleep(1 * time.Second)

	finalNumGoroutine := runtime.NumGoroutine()
	assert.Equal(t, initialNumGoroutine, finalNumGoroutine)
}

func TestExtraGetShardContentsFromNonHost(t *testing.T) {
	// GetShardContents should error if sent to a node that doesn't host the shard
	setup := MakeTestSetup(MakeTwoNodeMultiShard())

	response, err := setup.nodes["n2"].GetShardContents(context.Background(), &proto.GetShardContentsRequest{Shard: 1})
	assert.Nil(t, response)
	assert.NotNil(t, err)
	e, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, e.Code())

	setup.Shutdown()
}

func TestExtraGetShardContentsFromHost(t *testing.T) {
	// GetShardContents should work when getting from a node that hosts the shard
	// The TTL should be less than or equal to the set TTL
	setup := MakeTestSetup(MakeMultiShardSingleNode())

	key1 := "abc"
	value1 := "123"
	ttl1 := 10 * time.Second
	key2 := "def"
	value2 := "456"
	ttl2 := 20 * time.Second

	err := setup.NodeSet("n1", key1, value1, ttl1)
	assert.Nil(t, err)
	err = setup.NodeSet("n1", key2, value2, ttl2)
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)

	shard1 := kv.GetShardForKey(key1, setup.NumShards())
	response, err := setup.nodes["n1"].GetShardContents(context.Background(), &proto.GetShardContentsRequest{Shard: int32(shard1)})
	assert.Nil(t, err)
	assert.NotNil(t, response)
	assert.Len(t, response.Values, 1)
	assert.Equal(t, key1, response.Values[0].GetKey())
	assert.Equal(t, value1, response.Values[0].GetValue())
	assert.LessOrEqual(t, response.Values[0].GetTtlMsRemaining(), ttl1)

	shard2 := kv.GetShardForKey(key2, setup.NumShards())
	response, err = setup.nodes["n1"].GetShardContents(context.Background(), &proto.GetShardContentsRequest{Shard: int32(shard2)})
	assert.Nil(t, err)
	assert.NotNil(t, response)
	assert.Len(t, response.Values, 1)
	assert.Equal(t, key2, response.Values[0].GetKey())
	assert.Equal(t, value2, response.Values[0].GetValue())
	assert.LessOrEqual(t, response.Values[0].GetTtlMsRemaining(), ttl2)

	setup.Shutdown()
}

func TestExtraConcurrentGetShardContents(t *testing.T) {
	setup := MakeTestSetup(MakeMultiShardSingleNode())

	keys := RandomKeys(200, 10)
	vals := RandomKeys(200, 10)
	keySet := make(map[string]struct{})
	valSet := make(map[string]struct{})
	for i := range 200 {
		keySet[keys[i]] = struct{}{}
		valSet[vals[i]] = struct{}{}
		err := setup.NodeSet("n1", keys[i], vals[i], 100*time.Second)
		assert.Nil(t, err)
	}

	var wg sync.WaitGroup
	numGoros := 20
	numIters := 50
	wg.Add(numGoros)
	for i := 0; i < numGoros; i++ {
		go func(i int) {
			defer wg.Done()
			retrievedKeys := make(map[string]struct{})
			retrievedVals := make(map[string]struct{})
			for j := 0; j < numIters; j++ {
				randIndex := rand.IntN(setup.NumShards())
				for offset := 0; offset < setup.NumShards(); offset++ {
					shard := int32((randIndex+offset)%setup.NumShards() + 1)
					response, err := setup.nodes["n1"].GetShardContents(
						context.Background(),
						&proto.GetShardContentsRequest{Shard: shard},
					)
					assert.Nil(t, err)
					assert.NotNil(t, response)
					for _, v := range response.Values {
						assert.LessOrEqual(t, v.TtlMsRemaining, 100*time.Second)
						retrievedKeys[v.Key] = struct{}{}
						retrievedVals[v.Value] = struct{}{}
					}
				}
			}

			// GetShardResponse on all shards should get all key value pairs
			for key := range keySet {
				_, ok := retrievedKeys[key]
				assert.True(t, ok)
			}
			for val := range valSet {
				_, ok := retrievedVals[val]
				assert.True(t, ok)
			}
		}(i)
	}
	wg.Wait()

	setup.Shutdown()
}

func TestExtraHandleShardMapUpdateLoadBalance(t *testing.T) {
	// handleShardMapUpdate should be load balanced between nodes hosting the same shard
	// New note: this was actually able to find a bug in my load balancing implementation
	shardMap := kv.ShardMapState{
		NumShards: 4,
		Nodes:     makeNodeInfos(3),
		ShardsToNodes: map[int][]string{
			1: {"n1", "n2", "n3"},
			2: {"n1", "n2", "n3"},
		},
	}
	setup := MakeTestSetup(shardMap)

	values := []*proto.GetShardValue{
		{Key: "abc", Value: "123", TtlMsRemaining: 10000},
		{Key: "def", Value: "456", TtlMsRemaining: 10000},
		{Key: "ghi", Value: "789", TtlMsRemaining: 10000},
	}

	setup.clientPool.OverrideGetShardContentsResponse("n1", &proto.GetShardContentsResponse{Values: values})
	setup.clientPool.OverrideGetShardContentsResponse("n2", &proto.GetShardContentsResponse{Values: values})
	setup.clientPool.OverrideGetShardContentsResponse("n3", &proto.GetShardContentsResponse{Values: values})

	noN1ShardMap := kv.ShardMapState{
		NumShards: 4,
		Nodes:     makeNodeInfos(3),
		ShardsToNodes: map[int][]string{
			1: {"n2", "n3"},
			2: {"n2", "n3"},
		},
	}

	for range 100 {
		setup.shardMap.Update(&noN1ShardMap)
		time.Sleep(10 * time.Millisecond)
		setup.shardMap.Update(&shardMap)
		time.Sleep(10 * time.Millisecond)
	}

	n2Rpc := setup.clientPool.GetRequestsSent("n2")
	n3Rpc := setup.clientPool.GetRequestsSent("n3")

	assert.InDelta(t, n2Rpc, n3Rpc, 0.3*float64(max(n2Rpc, n3Rpc)))
}
