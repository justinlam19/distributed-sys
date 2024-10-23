package lab0

import (
	"context"
	"sync"
)

// MergeChannels should read from the channels `a` and `b`
// concurrently and write all values received to `out` as
// they are received.
//
// MergeChannels should run until all elements have been read from
// both `a` and `b`, then close `out` to signal that all results
// have been merged.
//
// The input parameters are guaranteed to be not `nil`.
//
// There are multiple ways to implement this method, any of which
// are valid as long as they meet the specification.
// If you are stuck, consider revisiting channels in Tour of Go:
//   - https://go.dev/tour/concurrency/4
//   - https://go.dev/tour/concurrency/5
func MergeChannels[T any](a <-chan T, b <-chan T, out chan<- T) {
	var wg sync.WaitGroup
	read := func(c <-chan T) {
		defer wg.Done()
		for msg := range c {
			out <- msg
		}
	}
	wg.Add(2)
	go read(a)
	go read(b)
	wg.Wait()
	close(out)
}

// MergeChannelsOrCancel provides similar semantics to MergeChannels, but
// allows for the caller to cancel processing by cancelling the context `ctx`.
// Results from channels `a` and `b` should be read concurrently and written
// to `out` until there are no more results in either channel, *or* `ctx` is
// done. If `ctx` is done and contains an error, it should be returned. In
// all other cases, `nil` should be returned.
//
// The input parameters are guaranteed to be not `nil`.
//
// For more details, read about contexts:
//   - https://pkg.go.dev/context
//   - https://www.digitalocean.com/community/tutorials/how-to-use-contexts-in-go#determining-if-a-context-is-done
//
// If the return value is confusing, read more about errors:
//   - https://go.dev/tour/methods/19
//
// It is expected that your implemented is similar to `MergeChannels`. You do
// not need to refactor to deduplicate your code, but you can if you want to.
func MergeChannelsOrCancel[T any](ctx context.Context, a <-chan T, b <-chan T, out chan<- T) error {
	var wg sync.WaitGroup
	read := func(c <-chan T) {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-c:
				if !ok {
					return
				}
				out <- msg
			case <-ctx.Done():
				return
			}
		}
	}
	wg.Add(2)
	go read(a)
	go read(b)
	wg.Wait()
	close(out)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Fetcher is an interface which mimics fetching from some source
// like a database, web service, or file system. Fetching could take
// considerable time.
//
// Fetch() should be called multiple times to keep fetching new data.
// Fetching is considered done once `false` is returned.
//
// You do not need to implement `Fetcher` in any way, just use the
// `Fetch()` method as part of `MergeFetches`.
type Fetcher interface {

	// Fetch returns two values:
	//  - new data and `true` when there is data available to be fetched
	//  - "" and `false` when fetching is done
	//
	// Fetch() should not be called again once `false` is returned
	//
	// For example, fetching all data from a fetcher:
	// ```
	// for {
	//     data, ok := fetcher.Fetch()
	//     if !ok {
	//         break
	//     }
	//     fmt.Println("data: " + data)
	// }
	// ```
	Fetch() (string, bool)
}

// MergeFetches is similar to `MergeChannels`, however you must merge results
// returned from a "Fetcher" instead of a channel. Consider Fetcher like an
// interface for fetching data from a database or web service. It may take
// significant amount of time.
//
// MergeFetches must fetch from both `a` and `b` concurrently and write results
// to `out` until both fetchers are "done" (have returned `false` from `Fetch()`).
// Once complete, `out` must be closed.
//
// We recommend using `sync.WaitGroup` and goroutines to implement `MergeFetches`.
// If you are stuck, consider reading the example for `WaitGroup` here:
//   - https://pkg.go.dev/sync#example-WaitGroup
func MergeFetches(a Fetcher, b Fetcher, out chan<- string) {
	var wg sync.WaitGroup
	read := func(f Fetcher) {
		defer wg.Done()
		for {
			msg, ok := f.Fetch()
			if !ok {
				return
			}
			out <- msg
		}
	}
	wg.Add(2)
	go read(a)
	go read(b)
	wg.Wait()
	close(out)
}
