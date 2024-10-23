package lab0_test

import (
	"context"
	"testing"
	"time"

	"cs426.cloud/lab0"
	"github.com/stretchr/testify/require"
)

func TestSemaphore(t *testing.T) {
	t.Run("semaphore basic", func(t *testing.T) {
		s := lab0.NewSemaphore()
		go func() {
			s.Post()
		}()
		err := s.Wait(context.Background())
		require.NoError(t, err)
	})
	t.Run("semaphore starts with zero available resources", func(t *testing.T) {
		s := lab0.NewSemaphore()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		err := s.Wait(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("semaphore post before wait does not block", func(t *testing.T) {
		s := lab0.NewSemaphore()
		s.Post()
		s.Post()
		s.Post()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		err := s.Wait(ctx)
		require.NoError(t, err)
	})
	t.Run("post after wait releases the wait", func(t *testing.T) {
		s := lab0.NewSemaphore()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		go func() {
			time.Sleep(20 * time.Millisecond)
			s.Post()
		}()
		err := s.Wait(ctx)
		require.NoError(t, err)
	})

	t.Run("multiple concurrent waiters", func(t *testing.T) {
		s := lab0.NewSemaphore()
		nWaiters := 5

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		results := make(chan error, nWaiters)
		for i := 0; i < nWaiters; i++ {
			go func() {
				err := s.Wait(ctx)
				results <- err
			}()
		}

		for i := 0; i < nWaiters; i++ {
			time.Sleep(10 * time.Millisecond)
			s.Post()
		}

		for i := 0; i < nWaiters; i++ {
			err := <-results
			require.NoError(t, err)
		}
	})

	t.Run("no post", func(t *testing.T) {
		s := lab0.NewSemaphore()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := s.Wait(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("wait cannot exceed post", func(t *testing.T) {
		s := lab0.NewSemaphore()
		s.Post()

		err := s.Wait(context.Background())
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err = s.Wait(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("context cancellation", func(t *testing.T) {
		s := lab0.NewSemaphore()
		waiterCount := 3

		ctx, cancel := context.WithCancel(context.Background())

		results := make(chan error, waiterCount)
		for i := 0; i < waiterCount; i++ {
			go func() {
				err := s.Wait(ctx)
				results <- err
			}()
		}

		time.Sleep(50 * time.Millisecond)
		cancel()

		for i := 0; i < waiterCount; i++ {
			err := <-results
			require.ErrorIs(t, err, context.Canceled)
		}
	})
}
