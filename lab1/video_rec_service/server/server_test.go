package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	fipb "cs426.yale.edu/lab1/failure_injection/proto"
	umc "cs426.yale.edu/lab1/user_service/mock_client"
	usl "cs426.yale.edu/lab1/user_service/server_lib"
	vmc "cs426.yale.edu/lab1/video_service/mock_client"
	vsl "cs426.yale.edu/lab1/video_service/server_lib"

	pb "cs426.yale.edu/lab1/video_rec_service/proto"
	sl "cs426.yale.edu/lab1/video_rec_service/server_lib"
	//
	// used for helper method unit tests
	// vpb "cs426.yale.edu/lab1/video_service/proto"
	// "cs426.yale.edu/lab1/user_service/proto"
)

/*
* Normally the video rec server would be closed after the function exits, e.g. defer server.Close() in each test
* but in order to make the tests pass the reference implementations that has been removed
 */

func TestServerBasic(t *testing.T) {
	vrOptions := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: true,
		DisableRetry:    true,
	}
	// You can specify failure injection options here or later send
	// SetInjectionConfigRequests using these mock clients
	uClient :=
		umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient :=
		vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions,
		uClient,
		vClient,
	)

	var userId uint64 = 204054
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
	)
	assert.True(t, err == nil)

	videos := out.Videos
	assert.Equal(t, 5, len(videos))
	assert.EqualValues(t, 1012, videos[0].VideoId)
	assert.Equal(t, "Harry Boehm", videos[1].Author)
	assert.EqualValues(t, 1209, videos[2].VideoId)
	assert.Equal(t, "https://video-data.localhost/blob/1309", videos[3].Url)
	assert.Equal(t, "precious here", videos[4].Title)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{
			// fail one in 1 request, i.e., always fail
			FailureRate: 1,
		},
	})

	// Since we disabled retry and fallback, we expect the VideoRecService to
	// throw an error since the VideoService is "down".
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
	)
	assert.False(t, err == nil)
}

// test whether the returned stats are correct
func TestStats(t *testing.T) {
	t.Run("initial stats are zero or NaN", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    50,
			DisableFallback: true,
			DisableRetry:    true,
		}
		uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stats, err := vrService.GetStats(ctx, &pb.GetStatsRequest{})
		assert.Nil(t, err)
		assert.Zero(t, stats.TotalRequests)
		assert.Zero(t, stats.TotalErrors)
		assert.Zero(t, stats.ActiveRequests)
		assert.Zero(t, stats.UserServiceErrors)
		assert.Zero(t, stats.VideoServiceErrors)
		assert.Zero(t, stats.StaleResponses)
		// assert NaN
		assert.True(t, stats.AverageLatencyMs != stats.AverageLatencyMs)
		assert.True(t, stats.P99LatencyMs != stats.P99LatencyMs)
	})

	t.Run("stats after concurrent calls", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    50,
			DisableFallback: true,
			DisableRetry:    true,
		}
		uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		numSuccess := 3
		numUserFails := 2
		numVideoFails := 1
		var wg sync.WaitGroup
		for range numSuccess {
			wg.Add(1)
			go func() {
				defer wg.Done()
				vrService.GetTopVideos(ctx, &pb.GetTopVideosRequest{UserId: 204054, Limit: 5})
			}()
		}
		wg.Wait()

		uClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
			Config: &fipb.InjectionConfig{FailureRate: 1},
		})
		for range numUserFails {
			wg.Add(1)
			go func() {
				defer wg.Done()
				vrService.GetTopVideos(ctx, &pb.GetTopVideosRequest{UserId: 204054, Limit: 5})
			}()
		}
		wg.Wait()

		uClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
			Config: &fipb.InjectionConfig{FailureRate: 0},
		})
		vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
			Config: &fipb.InjectionConfig{FailureRate: 1},
		})
		for range numVideoFails {
			wg.Add(1)
			go func() {
				defer wg.Done()
				vrService.GetTopVideos(ctx, &pb.GetTopVideosRequest{UserId: 204054, Limit: 5})
			}()
		}
		wg.Wait()

		stats, err := vrService.GetStats(ctx, &pb.GetStatsRequest{})
		assert.Nil(t, err)
		// expected number of requests and errors
		// since retry is disabled, numUserFails + numVideoFails should equal total errors
		assert.Equal(t, uint64(numSuccess+numUserFails+numVideoFails), stats.TotalRequests)
		assert.Equal(t, uint64(numUserFails+numVideoFails), stats.TotalErrors)
		assert.Equal(t, uint64(numUserFails), stats.UserServiceErrors)
		assert.Equal(t, uint64(numVideoFails), stats.VideoServiceErrors)
		// latency values are not NaN
		assert.True(t, stats.AverageLatencyMs == stats.AverageLatencyMs)
		assert.True(t, stats.P99LatencyMs == stats.P99LatencyMs)
		// no active requests
		assert.Zero(t, stats.ActiveRequests)
	})
}

// test whether the server errors appropriately
// when fallback and retry are disabled
func TestErrorHandling(t *testing.T) {
	t.Run("error when user service fails", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    50,
			DisableFallback: true,
			DisableRetry:    true,
		}
		uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)

		var userId uint64 = 204054
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		uClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
			Config: &fipb.InjectionConfig{FailureRate: 1},
		})

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := vrService.GetTopVideos(
			ctx,
			&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
		)
		assert.NotNil(t, err)
	})

	t.Run("error when video service fails", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    50,
			DisableFallback: true,
			DisableRetry:    true,
		}
		uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)

		var userId uint64 = 204054
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
			Config: &fipb.InjectionConfig{FailureRate: 1},
		})

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := vrService.GetTopVideos(
			ctx,
			&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
		)
		assert.NotNil(t, err)
	})

	t.Run("error if limit is negative", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    50,
			DisableFallback: true,
			DisableRetry:    true,
		}
		uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)

		var userId uint64 = 204054
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := vrService.GetTopVideos(
			ctx,
			&pb.GetTopVideosRequest{UserId: userId, Limit: -1},
		)
		assert.NotNil(t, err)
	})
}

// test whether enabling retry works as expected
func TestRetry(t *testing.T) {
	vrOptions := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: true,
		DisableRetry:    false,
	}
	uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions,
		uClient,
		vClient,
	)

	var userId uint64 = 204054
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{FailureRate: 1},
	})

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
	)
	assert.NotNil(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stats, err := vrService.GetStats(ctx, &pb.GetStatsRequest{})
	assert.Nil(t, err)
	// more video errors than total errors because of retry
	assert.Greater(t, stats.VideoServiceErrors, stats.TotalErrors)
}

// test whether falling back to cache works
func TestFallback(t *testing.T) {
	vrOptions := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: false,
		DisableRetry:    true,
	}
	uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions,
		uClient,
		vClient,
	)
	go vrService.ContinuallyRefreshCache()
	time.Sleep(1 * time.Second) // make sure cache has data

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{FailureRate: 1},
	})

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var userId uint64 = 204054
	_, err := vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
	)
	// no error because of fallback
	assert.Nil(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stats, err := vrService.GetStats(ctx, &pb.GetStatsRequest{})
	assert.Nil(t, err)
	// 1 stale response
	assert.Equal(t, uint64(1), stats.StaleResponses)
}

// test batched retrieval in different cases
func TestBatching(t *testing.T) {
	t.Run("batch size larger than allowed", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    3,
			DisableFallback: true,
			DisableRetry:    true,
		}
		userOptions := *usl.DefaultUserServiceOptions()
		userOptions.MaxBatchSize = 2
		uClient := umc.MakeMockUserServiceClient(userOptions)
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		videos, err := vrService.GetTopVideos(
			ctx,
			&pb.GetTopVideosRequest{
				UserId: 204054,
				Limit:  5,
			},
		)
		assert.Nil(t, videos)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "batch size")
	})

	t.Run("number of videos is not multiple of batch size", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    7,
			DisableFallback: true,
			DisableRetry:    true,
		}
		userOptions := *usl.DefaultUserServiceOptions()
		uClient := umc.MakeMockUserServiceClient(userOptions)
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := vrService.GetTopVideos(
			ctx,
			&pb.GetTopVideosRequest{
				UserId: 204054,
				Limit:  5,
			},
		)
		assert.Nil(t, err)
	})
}

// full test of GetTopVideos, fallback and retry enabled
// uses mocks
func TestOverall(t *testing.T) {
	vrOptions := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: false,
		DisableRetry:    false,
	}
	uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions,
		uClient,
		vClient,
	)

	go vrService.ContinuallyRefreshCache()

	var userId uint64 = 204054
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
	)
	assert.Nil(t, err) // initial get top videos should succeed

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{FailureRate: 1},
	})

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
	)
	assert.Nil(t, err) // due to fallback

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stats, err := vrService.GetStats(ctx, &pb.GetStatsRequest{})
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), stats.TotalRequests)
	assert.Equal(t, uint64(0), stats.UserServiceErrors)
	// 0 total errors because of fallback
	assert.Equal(t, uint64(0), stats.TotalErrors)
	// 1 stale response because of caching
	assert.Equal(t, uint64(1), stats.StaleResponses)
	// 2 video errors because of retry
	assert.Equal(t, uint64(2), stats.VideoServiceErrors)
}

/* unit tests for custom helper methods
*
// tests whether the input slice is batched correctly
func TestBatchIDs(t *testing.T) {
	t.Run("batch size less than length of slice", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    3,
			DisableFallback: true,
			DisableRetry:    true,
		}
		userOptions := *usl.DefaultUserServiceOptions()
		uClient := umc.MakeMockUserServiceClient(userOptions)
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)
		defer vrService.Close()

		ids := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		batches := vrService.BatchIDs(ids)
		expected_output := [][]uint64{
			{0, 1, 2},
			{3, 4, 5},
			{6, 7, 8},
			{9, 10},
		}
		assert.Equal(t, expected_output, batches)
	})

	t.Run("batch size greater than length of slice", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    50,
			DisableFallback: true,
			DisableRetry:    true,
		}
		userOptions := *usl.DefaultUserServiceOptions()
		uClient := umc.MakeMockUserServiceClient(userOptions)
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)
		defer vrService.Close()

		ids := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		batches := vrService.BatchIDs(ids)
		assert.Equal(t, [][]uint64{ids}, batches)
	})
}

// tests whether getting user info in batches works for different cases
func TestBatchedGetUser(t *testing.T) {
	t.Run("batch size larger than allowed", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    3,
			DisableFallback: true,
			DisableRetry:    true,
		}
		userOptions := *usl.DefaultUserServiceOptions()
		userOptions.MaxBatchSize = 2
		uClient := umc.MakeMockUserServiceClient(userOptions)
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)
		defer vrService.Close()

		userIds := []uint64{204054, 203584, 203115}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		users, err := vrService.BatchedGetUser(ctx, userIds, false)
		assert.Nil(t, users)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "batch size")
	})

	t.Run("number of videos is not multiple of batch size", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    2,
			DisableFallback: true,
			DisableRetry:    true,
		}
		userOptions := *usl.DefaultUserServiceOptions()
		uClient := umc.MakeMockUserServiceClient(userOptions)
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)
		defer vrService.Close()

		userIds := []uint64{204054, 203584, 203115}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		users, err := vrService.BatchedGetUser(ctx, userIds, false)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(users))
		for i, user := range users {
			assert.Equal(t, userIds[i], user.UserId)
		}
	})

	t.Run("number of videos is less than batch size", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    2,
			DisableFallback: true,
			DisableRetry:    true,
		}
		userOptions := *usl.DefaultUserServiceOptions()
		uClient := umc.MakeMockUserServiceClient(userOptions)
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)
		defer vrService.Close()

		userIds := []uint64{204054}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		users, err := vrService.BatchedGetUser(ctx, userIds, false)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(users))
		assert.Equal(t, userIds[0], users[0].UserId)
	})
}

// tests whether retrieving videos in batches works for various cases
func TestBatchedGetVideo(t *testing.T) {
	t.Run("batch size larger than allowed", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    3,
			DisableFallback: true,
			DisableRetry:    true,
		}
		videoOptions := *vsl.DefaultVideoServiceOptions()
		videoOptions.MaxBatchSize = 2
		uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
		vClient := vmc.MakeMockVideoServiceClient(videoOptions)
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)
		defer vrService.Close()

		videoIds := []uint64{1012, 1196, 1161}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		videos, err := vrService.BatchedGetVideo(ctx, videoIds, false)
		assert.Nil(t, videos)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "batch size")
	})

	t.Run("number of videos is not multiple of batch size", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    2,
			DisableFallback: true,
			DisableRetry:    true,
		}
		videoOptions := *vsl.DefaultVideoServiceOptions()
		videoOptions.MaxBatchSize = 3
		uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
		vClient := vmc.MakeMockVideoServiceClient(videoOptions)
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)
		defer vrService.Close()

		videoIds := []uint64{1012, 1196, 1161}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		videos, err := vrService.BatchedGetVideo(ctx, videoIds, false)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(videos))
		for i, video := range videos {
			assert.Equal(t, videoIds[i], video.VideoId)
		}
	})

	t.Run("number of videos is less than batch size", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    2,
			DisableFallback: true,
			DisableRetry:    true,
		}
		videoOptions := *vsl.DefaultVideoServiceOptions()
		videoOptions.MaxBatchSize = 1
		uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
		vClient := vmc.MakeMockVideoServiceClient(videoOptions)
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)
		defer vrService.Close()

		videoIds := []uint64{1012}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		videos, err := vrService.BatchedGetVideo(ctx, videoIds, false)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(videos))
		assert.Equal(t, videoIds[0], videos[0].VideoId)
	})
}

// tests whether returned videos are correctly truncated
func TestRankAndTruncate(t *testing.T) {
	t.Run("limit is greater than number of videos", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    50,
			DisableFallback: true,
			DisableRetry:    true,
		}
		uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)
		defer vrService.Close()

		// bypass the ranker
		var userInfo *proto.UserInfo = nil

		inputVideos := []*vpb.VideoInfo{
			{VideoId: 1},
			{VideoId: 2},
		}
		staleResponse := false
		videoResponse, err := vrService.RankAndTruncate(userInfo, 50, inputVideos, staleResponse)
		assert.Nil(t, err)
		assert.Equal(t, inputVideos, videoResponse.Videos)
		assert.Equal(t, staleResponse, videoResponse.StaleResponse)
	})

	t.Run("limit is less than number of videos", func(t *testing.T) {
		vrOptions := sl.VideoRecServiceOptions{
			MaxBatchSize:    50,
			DisableFallback: true,
			DisableRetry:    true,
		}
		uClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
		vClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
		vrService := sl.MakeVideoRecServiceServerWithMocks(
			vrOptions,
			uClient,
			vClient,
		)
		defer vrService.Close()

		// bypass the ranker
		var userInfo *proto.UserInfo = nil

		inputVideos := []*vpb.VideoInfo{
			{VideoId: 1},
			{VideoId: 2},
			{VideoId: 3},
			{VideoId: 4},
			{VideoId: 5},
		}
		staleResponse := true
		expectedVideos := []*vpb.VideoInfo{
			{VideoId: 1},
			{VideoId: 2},
		}

		videoResponse, err := vrService.RankAndTruncate(userInfo, len(expectedVideos), inputVideos, staleResponse)
		assert.Nil(t, err)
		assert.Equal(t, expectedVideos, videoResponse.Videos)
		assert.Equal(t, staleResponse, videoResponse.StaleResponse)
	})
}
*
*/
