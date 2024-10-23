package lab0_test

import (
	"context"
	"sync/atomic"
	"testing"

	"cs426.cloud/lab0"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type intQueue interface {
	Push(i int)
	Pop() (int, bool)
}

type stringQueue interface {
	Push(s string)
	Pop() (string, bool)
}

func runIntQueueTests(t *testing.T, q intQueue) {
	t.Run("queue starts empty", func(t *testing.T) {
		_, ok := q.Pop()
		require.False(t, ok)
	})

	t.Run("simple push pop", func(t *testing.T) {
		q.Push(1)
		q.Push(2)
		q.Push(3)

		x, ok := q.Pop()
		require.True(t, ok)
		require.Equal(t, 1, x)

		x, ok = q.Pop()
		require.True(t, ok)
		require.Equal(t, 2, x)

		x, ok = q.Pop()
		require.True(t, ok)
		require.Equal(t, 3, x)
	})

	t.Run("queue empty again", func(t *testing.T) {
		_, ok := q.Pop()
		require.False(t, ok)
	})

	t.Run("push and pop", func(t *testing.T) {
		for i := 1; i <= 10; i++ {
			q.Push(i)
		}
		for i := 1; i <= 5; i++ {
			x, ok := q.Pop()
			require.True(t, ok)
			require.Equal(t, i, x)
		}

		for i := 11; i <= 100; i++ {
			q.Push(i)
		}

		x, ok := q.Pop()
		require.True(t, ok)
		require.Equal(t, 6, x)

		q.Push(0)
		for i := 7; i <= 100; i++ {
			x, ok := q.Pop()
			require.True(t, ok)
			require.Equal(t, i, x)
		}
		x, ok = q.Pop()
		require.True(t, ok)
		require.Equal(t, 0, x)

		_, ok = q.Pop()
		require.False(t, ok)

		_, ok = q.Pop()
		require.False(t, ok)
	})
}

func runStringQueueTests(t *testing.T, q stringQueue) {
	t.Run("with strings", func(t *testing.T) {
		_, ok := q.Pop()
		require.False(t, ok)

		q.Push("hello!")

		v, ok := q.Pop()
		require.True(t, ok)
		require.Equal(t, "hello!", v)
	})
}

func runConcurrentQueueTests(t *testing.T, q *lab0.ConcurrentQueue[int]) {
	const concurrency = 16
	const pushes = 1000

	ctx := context.Background()
	t.Run("concurrent pushes", func(t *testing.T) {
		eg, _ := errgroup.WithContext(ctx)
		for i := 0; i < concurrency; i++ {
			eg.Go(func() error {
				for i := 0; i < pushes; i++ {
					q.Push(1234)
				}
				return nil
			})
		}
		eg.Wait()

		for i := 0; i < concurrency*pushes; i++ {
			v, ok := q.Pop()
			require.True(t, ok)
			require.Equal(t, 1234, v)
		}
		_, ok := q.Pop()
		require.False(t, ok)
	})

	t.Run("concurrent pops", func(t *testing.T) {
		eg, _ := errgroup.WithContext(ctx)
		for i := 0; i < concurrency*pushes/2; i++ {
			q.Push(i)
		}

		var sum int64
		var found int64
		for i := 0; i < concurrency; i++ {
			eg.Go(func() error {
				for i := 0; i < pushes; i++ {
					v, ok := q.Pop()
					if ok {
						atomic.AddInt64(&sum, int64(v))
						atomic.AddInt64(&found, 1)
					}
				}
				return nil
			})
		}
		eg.Wait()
		// In the end, we should have exactly concurrency * pushes/2 elements in popped
		require.Equal(t, int64(concurrency*pushes/2), found)
		// Mathematical SUM(i=0...7999)
		require.Equal(t, int64(31996000), sum)
	})
}

func runAdditionalIntQueueTests(t *testing.T, q intQueue) {
	t.Run("pop from empty queue after multiple pushes and pops", func(t *testing.T) {
		q.Push(1)
		q.Push(2)

		x, ok := q.Pop()
		require.True(t, ok)
		require.Equal(t, 1, x)

		x, ok = q.Pop()
		require.True(t, ok)
		require.Equal(t, 2, x)

		_, ok = q.Pop()
		require.False(t, ok)

		_, ok = q.Pop()
		require.False(t, ok)
	})

	t.Run("alternating push-pop calls", func(t *testing.T) {
		q.Push(1)
		x, ok := q.Pop()
		require.True(t, ok)
		require.Equal(t, 1, x)

		q.Push(2)
		x, ok = q.Pop()
		require.True(t, ok)
		require.Equal(t, 2, x)

		q.Push(3)
		q.Push(4)
		x, ok = q.Pop()
		require.True(t, ok)
		require.Equal(t, 3, x)
		x, ok = q.Pop()
		require.True(t, ok)
		require.Equal(t, 4, x)
	})

	t.Run("handle large number of pushes and pops", func(t *testing.T) {
		for i := 0; i < 10000; i++ {
			q.Push(i)
		}

		for i := 0; i < 10000; i++ {
			x, ok := q.Pop()
			require.True(t, ok)
			require.Equal(t, i, x)
		}

		_, ok := q.Pop()
		require.False(t, ok)
	})
}

func runAdditionalConcurrentQueueTests(t *testing.T, q *lab0.ConcurrentQueue[int]) {
	const concurrency = 32
	const iterations = 5000

	ctx := context.Background()

	t.Run("concurrent push and pop mix", func(t *testing.T) {
		hasItems := true
		for hasItems {
			_, hasItems = q.Pop()
		}

		eg, _ := errgroup.WithContext(ctx)

		for i := 0; i < concurrency; i++ {
			eg.Go(func() error {
				for i := 0; i < iterations; i++ {
					q.Push(i)
				}
				return nil
			})
			eg.Go(func() error {
				for i := 0; i < iterations; i++ {
					q.Pop()
				}
				return nil
			})
		}

		err := eg.Wait()
		require.NoError(t, err)
	})

	t.Run("all pops after concurrent pushes", func(t *testing.T) {
		hasItems := true
		for hasItems {
			_, hasItems = q.Pop()
		}

		eg, _ := errgroup.WithContext(ctx)

		for i := 0; i < concurrency; i++ {
			eg.Go(func() error {
				for j := 0; j < iterations; j++ {
					q.Push(j)
				}
				return nil
			})
		}

		err := eg.Wait()
		require.NoError(t, err)

		for i := 0; i < concurrency*iterations; i++ {
			_, ok := q.Pop()
			require.True(t, ok)
		}

		_, ok := q.Pop()
		require.False(t, ok)
	})
}

func TestGenericQueue(t *testing.T) {
	q := lab0.NewQueue[int]()

	require.NotNil(t, q)
	runIntQueueTests(t, q)
	runAdditionalIntQueueTests(t, q)

	qs := lab0.NewQueue[string]()
	require.NotNil(t, qs)
	runStringQueueTests(t, qs)
}

func TestConcurrentQueue(t *testing.T) {
	q := lab0.NewConcurrentQueue[int]()
	require.NotNil(t, q)
	runIntQueueTests(t, q)
	runAdditionalIntQueueTests(t, q)

	qs := lab0.NewConcurrentQueue[string]()
	require.NotNil(t, qs)
	runStringQueueTests(t, qs)

	qc := lab0.NewConcurrentQueue[int]()
	require.NotNil(t, qc)
	runConcurrentQueueTests(t, qc)
	runAdditionalConcurrentQueueTests(t, qc)
}
