package lab0

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// ParallelFetcher manages concurrent fetches of resources that the underlying Fetcher interacts with.
// The ParallelFetcher imposes an upper limit allowed on the number of concurrent (and parallel) fetches.
//
// You can use a `semaphore.Weighted` with `context.Background()` to handle the blocking.
type ParallelFetcher struct {
	fetcher Fetcher
	// Add your fields here
	sem *semaphore.Weighted
}

// ParallelFetcher ensures that no more than maxConcurrentLimit clients call `Fetcher.Fetch()` at any given time.
// Additional concurrent calls to `ParallelFetcher.Fetch()` should block until the underlying Fetcher
// becomes available (i.e., one of the previous Fetcher.Fetch() finishes).
//
// You may assume the underlying `Fetcher.Fetch()` is thread-safe.
func NewParallelFetcher(fetcher Fetcher, maxConcurrencyLimit int) *ParallelFetcher {
	return &ParallelFetcher{
		fetcher: fetcher,
		// Add more initialization here
		sem: semaphore.NewWeighted(int64(maxConcurrencyLimit)),
	}
}

// Addendum to the `Fetcher.Fetch()` contract: Fetch() should not be called again
// once `false` is returned; *however*, it is OK to have Fetch()s that are already in progress
// (which will also return false).
func (pf *ParallelFetcher) Fetch() (string, bool) {
	// Add your implementation here
	pf.sem.Acquire(context.Background(), 1)
	defer pf.sem.Release(1)
	return pf.fetcher.Fetch()
}
