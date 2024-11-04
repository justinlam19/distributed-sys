package kv

import (
	"hash/fnv"
)

/// This file can be used for any common code you want to define and separate
/// out from server.go or client.go

func GetShardForKey(key string, numShards int) int {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return int(hasher.Sum32())%numShards + 1
}

type IntSet struct {
	set map[int]struct{}
}

func (s *IntSet) Contains(v int) bool {
	_, ok := s.set[v]
	return ok
}

func (s *IntSet) Add(v int) {
	s.set[v] = struct{}{}
}

func (s *IntSet) Remove(v int) {
	delete(s.set, v)
}

func (s *IntSet) Size() int {
	return len(s.set)
}

func NewSet() *IntSet {
	return &IntSet{
		set: make(map[int]struct{}),
	}
}
