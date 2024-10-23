package lab0_test

import (
	"context"
	"sync"
	"testing"

	"cs426.cloud/lab0"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func chanToSlice[T any](ch chan T) []T {
	vals := make([]T, 0)
	for item := range ch {
		vals = append(vals, item)
	}
	return vals
}

type mergeFunc = func(chan string, chan string, chan string)

func runMergeTest(t *testing.T, merge mergeFunc) {
	t.Run("empty channels", func(t *testing.T) {
		a := make(chan string)
		b := make(chan string)
		out := make(chan string)
		close(a)
		close(b)

		merge(a, b, out)
		// If your lab0 hangs here, make sure you are closing your channels!
		require.Empty(t, chanToSlice(out))
	})

	// Please write your own tests
	t.Run("multiple items written to channels", func(t *testing.T) {
		a := make(chan string, 5)
		b := make(chan string, 5)
		out := make(chan string, 10)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			merge(a, b, out)
		}()

		a <- "a1"
		b <- "b1"
		a <- "a2"
		b <- "b2"
		close(a)
		close(b)

		wg.Wait()
		require.ElementsMatch(t, []string{"a1", "b1", "a2", "b2"}, chanToSlice(out))
	})

	t.Run("one closed channel", func(t *testing.T) {
		a := make(chan string)
		b := make(chan string)
		out := make(chan string)
		close(b)
		go func() {
			a <- "a"
			close(a)
		}()
		go merge(a, b, out)
		require.ElementsMatch(t, []string{"a"}, chanToSlice(out))
	})
}

func TestMergeChannels(t *testing.T) {
	runMergeTest(t, func(a, b, out chan string) {
		lab0.MergeChannels(a, b, out)
	})
}

func TestMergeOrCancel(t *testing.T) {
	runMergeTest(t, func(a, b, out chan string) {
		_ = lab0.MergeChannelsOrCancel(context.Background(), a, b, out)
	})

	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		a := make(chan string, 1)
		b := make(chan string, 1)
		out := make(chan string, 10)

		eg, _ := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			return lab0.MergeChannelsOrCancel(ctx, a, b, out)
		})
		err := eg.Wait()
		a <- "a"
		b <- "b"

		require.Error(t, err)
		require.Equal(t, []string{}, chanToSlice(out))
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		a := make(chan string)
		b := make(chan string)
		out := make(chan string, 10)

		eg, _ := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			return lab0.MergeChannelsOrCancel(ctx, a, b, out)
		})
		a <- "a"
		b <- "b"
		cancel()

		err := eg.Wait()
		require.Error(t, err)
		require.Equal(t, []string{"a", "b"}, chanToSlice(out))
	})
}

type channelFetcher struct {
	ch chan string
}

func newChannelFetcher(ch chan string) *channelFetcher {
	return &channelFetcher{ch: ch}
}

func (f *channelFetcher) Fetch() (string, bool) {
	v, ok := <-f.ch
	return v, ok
}

func TestMergeFetches(t *testing.T) {
	runMergeTest(t, func(a, b, out chan string) {
		lab0.MergeFetches(newChannelFetcher(a), newChannelFetcher(b), out)
	})
}

func TestMergeFetchesAdditional(t *testing.T) {
	t.Run("more items than channel capacity plus goroutine", func(t *testing.T) {
		a := make(chan string, 3)
		b := make(chan string, 3)
		out := make(chan string, 6)

		go func() {
			a <- "a1"
			b <- "b1"
			a <- "a2"
			b <- "b2"
			a <- "a3"
			b <- "b3"
			a <- "a4"
			b <- "b4"
			close(a)
			close(b)
		}()
		go lab0.MergeFetches(newChannelFetcher(a), newChannelFetcher(b), out)
		result := chanToSlice(out)
		require.ElementsMatch(t, []string{"a1", "b1", "a2", "b2", "a3", "b3", "a4", "b4"}, result)
	})

	t.Run("one fetcher finishes early", func(t *testing.T) {
		a := make(chan string, 3)
		b := make(chan string, 3)
		out := make(chan string, 6)

		go func() {
			a <- "a1"
			a <- "a2"
			close(a)
			b <- "b1"
			b <- "b2"
			close(b)
		}()

		lab0.MergeFetches(newChannelFetcher(a), newChannelFetcher(b), out)
		require.ElementsMatch(t, []string{"a1", "a2", "b1", "b2"}, chanToSlice(out))
	})
}
