package raft

import (
	"math/rand"
	"testing"
	"time"
)

func TestSimultaneousElections(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (Custom): simultaneous elections")

	// disconnect all servers
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}

	// reconnect two servers at (roughly) the same time
	cfg.connect(0)
	cfg.connect(1)

	// only one leader should be elected
	cfg.checkOneLeader()

	cfg.end()
}

func TestHighLoad(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (Custom): high load sent to leader")

	leader := cfg.checkOneLeader()

	// high log entry load sent to leader
	for i := range 999 {
		cfg.rafts[leader].Start(i)
	}

	// the raft cluster should still work
	cfg.one(rand.Int(), servers, true)

	cfg.end()
}

func TestLeaderDisconnectUnderHighLoad(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (Custom): leader disconnects while high load")

	leader := cfg.checkOneLeader()

	// high log entry load sent to leader
	for i := range 999 {
		cfg.rafts[leader].Start(i)
	}

	// leader disconnects
	cfg.disconnect(leader)

	// a single leader should be elected
	// everything should still be in agreement
	cfg.checkOneLeader()
	cfg.one(rand.Int(), servers-1, true)

	cfg.end()
}

func TestDivergentLogs(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (Custom): nodes with diverging logs")

	// disconnect two followers and do agreement
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.one(1, servers-2, false)

	// partition the leader and reconnect the two followers
	cfg.disconnect(leader)
	cfg.connect((leader + 1) % 5)
	cfg.connect((leader + 2) % 5)
	cfg.checkOneLeader()
	cfg.one(2, servers-1, false)

	// reconnect the leader
	cfg.connect(leader)

	// the nodes should all agree
	cfg.one(3, servers, true)

	cfg.end()
}

func TestDelayedFollower(t *testing.T) {
	servers := 5
	cfg := make_config(t, 5, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (Custom): delayed follower")

	// disconnect a follower
	leader := cfg.checkOneLeader()
	follower := (leader + 1) % servers
	cfg.disconnect(follower)

	// advance the term by holding multiple elections
	for range 3 {
		cfg.disconnect(leader)
		time.Sleep(RaftElectionTimeout)
		cfg.connect(leader)
		leader = cfg.checkOneLeader()
	}

	// reconnect the isolated follower
	cfg.connect(follower)
	time.Sleep(1 * time.Second)

	// they should still agree on the same currentTerm
	cfg.checkTerms()

	cfg.end()
}

func TestRepeatedLeaderFailure(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true, true)
	defer cfg.cleanup()

	cfg.begin("Test (Custom): repeated leader failure")

	// repeatedly start agreement at leader and disconnect the leader
	var leader int
	disconnected := -1
	for i := range 10 {
		leader = cfg.checkOneLeader()
		if disconnected >= 0 {
			cfg.connect(disconnected)
		}
		cfg.rafts[leader].Start(i)
		disconnected = leader
		cfg.disconnect(disconnected)
	}

	// reconnect the disconnected node
	// there should only be one leader
	// agreement should still be reached
	cfg.connect(disconnected)
	cfg.checkOneLeader()
	cfg.one(100, servers, false)

	cfg.end()
}
