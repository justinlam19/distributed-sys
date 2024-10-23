package server_lib

import (
	"context"
	"log"
	"math"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	ranker "cs426.yale.edu/lab1/ranker"
	umc "cs426.yale.edu/lab1/user_service/mock_client"
	upb "cs426.yale.edu/lab1/user_service/proto"
	pb "cs426.yale.edu/lab1/video_rec_service/proto"
	vmc "cs426.yale.edu/lab1/video_service/mock_client"
	vpb "cs426.yale.edu/lab1/video_service/proto"
	tdigest "github.com/influxdata/tdigest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// concurrent t-digest for computing quantile
type ConcurrentTDigest struct {
	digest *tdigest.TDigest
	mu     sync.RWMutex
}

func NewAtomicTDigest(compression float64) *ConcurrentTDigest {
	return &ConcurrentTDigest{
		digest: tdigest.NewWithCompression(compression),
	}
}

func (ctd *ConcurrentTDigest) Add(value float64, weight float64) {
	ctd.mu.Lock()
	defer ctd.mu.Unlock()
	ctd.digest.Add(value, weight)
}

func (ctd *ConcurrentTDigest) Quantile(quantile float64) float64 {
	ctd.mu.RLock()
	defer ctd.mu.RUnlock()
	return ctd.digest.Quantile(quantile)
}

// concurrent cache for storing trending videos
type ConcurrentCache struct {
	cache []*vpb.VideoInfo
	mu    sync.RWMutex
}

func NewConcurrentCache() *ConcurrentCache {
	return &ConcurrentCache{}
}

func (cc *ConcurrentCache) Set(videos []*vpb.VideoInfo) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.cache = make([]*vpb.VideoInfo, len(videos))
	copy(cc.cache, videos)
}

func (cc *ConcurrentCache) Retrieve() []*vpb.VideoInfo {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	videos := make([]*vpb.VideoInfo, len(cc.cache))
	copy(videos, cc.cache)
	return videos
}

type VideoRecServiceOptions struct {
	// Server address for the UserService"
	UserServiceAddr string
	// Server address for the VideoService
	VideoServiceAddr string
	// Maximum size of batches sent to UserService and VideoService
	MaxBatchSize int
	// If set, disable fallback to cache
	DisableFallback bool
	// If set, disable all retries
	DisableRetry bool
	// Size of client pool
	ClientPoolSize int
}

type VideoRecServiceServer struct {
	pb.UnimplementedVideoRecServiceServer
	options VideoRecServiceOptions
	// Add any data you want here
	// connections and clients
	userConns        []*grpc.ClientConn
	userClients      []upb.UserServiceClient
	videoConns       []*grpc.ClientConn
	videoClients     []vpb.VideoServiceClient
	userClientIndex  atomic.Uint64
	videoClientIndex atomic.Uint64

	// stats
	totalRequests      atomic.Uint64
	totalErrors        atomic.Uint64
	activeRequests     atomic.Uint64
	userServiceErrors  atomic.Uint64
	videoServiceErrors atomic.Uint64
	totalLatency       atomic.Uint64
	staleResponses     atomic.Uint64
	latencyDigest      *ConcurrentTDigest

	// caching
	cache              *ConcurrentCache
	cancelRefreshCache context.CancelFunc
}

func MakeVideoRecServiceServer(options VideoRecServiceOptions) (*VideoRecServiceServer, error) {
	server := &VideoRecServiceServer{
		options: options,
		// Add any data to initialize here
		latencyDigest: NewAtomicTDigest(1000),
		cache:         NewConcurrentCache(),
	}

	// error if maxBatchSize less than 1
	if server.options.MaxBatchSize < 1 {
		statusError := status.Errorf(codes.InvalidArgument, "maxBatchSize must be at least 1")
		log.Printf("Error occured when making video rec server: %v", statusError.Error())
		server.totalErrors.Add(1)
		return nil, statusError
	}

	// userClient
	err := server.InitUserClient()
	if err != nil {
		statusError := status.Errorf(codes.Unavailable, "couldn't connect to user service: %v", err)
		log.Printf("Error occured when making video rec server: %v", statusError.Error())
		return nil, statusError
	}
	// videoClient
	err = server.InitVideoClient()
	if err != nil {
		statusError := status.Errorf(codes.Unavailable, "couldn't connect to video service: %v", err)
		log.Printf("Error occured when making video rec server: %v", statusError.Error())
		return nil, statusError
	}
	return server, nil
}

func MakeVideoRecServiceServerWithMocks(
	options VideoRecServiceOptions,
	mockUserServiceClient *umc.MockUserServiceClient,
	mockVideoServiceClient *vmc.MockVideoServiceClient,
) *VideoRecServiceServer {
	// Implement your own logic here
	return &VideoRecServiceServer{
		options:       options,
		userClients:   []upb.UserServiceClient{mockUserServiceClient},
		videoClients:  []vpb.VideoServiceClient{mockVideoServiceClient},
		latencyDigest: NewAtomicTDigest(1000),
		cache:         NewConcurrentCache(),
	}
}

func (server *VideoRecServiceServer) InitUserClient() error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	address := server.options.UserServiceAddr
	for range server.options.ClientPoolSize {
		conn, err := grpc.NewClient(address, opts...)
		// retry once for transient errors
		if err != nil && !server.options.DisableRetry {
			server.userServiceErrors.Add(1)
			conn, err = grpc.NewClient(address, opts...)
		}
		if err == nil {
			server.userConns = append(server.userConns, conn)
			server.userClients = append(server.userClients, upb.NewUserServiceClient(conn))
		} else {
			server.totalErrors.Add(1)
			server.userServiceErrors.Add(1)
			return err
		}
	}
	return nil
}

func (server *VideoRecServiceServer) InitVideoClient() error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	address := server.options.VideoServiceAddr
	for range server.options.ClientPoolSize {
		conn, err := grpc.NewClient(address, opts...)
		// retry once for transient errors
		if err != nil && !server.options.DisableRetry {
			server.userServiceErrors.Add(1)
			conn, err = grpc.NewClient(address, opts...)
		}
		if err == nil {
			server.videoConns = append(server.videoConns, conn)
			server.videoClients = append(server.videoClients, vpb.NewVideoServiceClient(conn))
		} else {
			server.totalErrors.Add(1)
			server.videoServiceErrors.Add(1)
			return err
		}
	}
	return nil
}

// called by outside function to close connections and stop cache refresh
func (server *VideoRecServiceServer) Close() {
	if server.cancelRefreshCache != nil {
		server.cancelRefreshCache()
	}
	if server.userConns != nil {
		for _, conn := range server.userConns {
			if conn != nil {
				conn.Close()
			}
		}
	}
	if server.videoConns != nil {
		for _, conn := range server.videoConns {
			if conn != nil {
				conn.Close()
			}
		}
	}
}

func (server *VideoRecServiceServer) GetUserClientRoundRobin() upb.UserServiceClient {
	next := server.userClientIndex.Add(1) - 1
	index := int(next % uint64(len(server.userClients)))
	userClient := server.userClients[index]
	return userClient
}

func (server *VideoRecServiceServer) GetVideoClientRoundRobin() vpb.VideoServiceClient {
	next := server.videoClientIndex.Add(1) - 1
	index := int(next % uint64(len(server.videoClients)))
	videoClient := server.videoClients[index]
	return videoClient
}

func (server *VideoRecServiceServer) BatchIDs(ids []uint64) [][]uint64 {
	i := 0
	j := min(server.options.MaxBatchSize, len(ids))
	var batchedIDs [][]uint64
	for j <= len(ids) {
		batchedIDs = append(batchedIDs, ids[i:j])
		i = j
		// increment j by at least 1, to make sure the loop condition fails at some point
		// j will increment by maxBatchSize until it reaches the total number of users
		j += max(1, min(server.options.MaxBatchSize, len(ids)-i))
	}
	return batchedIDs
}

func (server *VideoRecServiceServer) BatchedGetUser(
	ctx context.Context,
	userClient upb.UserServiceClient,
	userIDs []uint64,
	retry bool,
) ([]*upb.UserInfo, error) {
	var users []*upb.UserInfo
	batches := server.BatchIDs(userIDs)
	for _, batch := range batches {
		userResponse, err := userClient.GetUser(
			ctx,
			&upb.GetUserRequest{UserIds: batch},
		)
		// retry for transient error
		if err != nil {
			server.userServiceErrors.Add(1)
			if !retry || server.options.DisableRetry {
				return nil, err
			}
			userResponse, err = userClient.GetUser(
				ctx,
				&upb.GetUserRequest{UserIds: batch},
			)
			if err != nil {
				server.userServiceErrors.Add(1)
				return nil, err
			}
		}
		users = append(users, userResponse.Users...)
	}
	return users, nil
}

func (server *VideoRecServiceServer) BatchedGetVideo(
	ctx context.Context,
	videoClient vpb.VideoServiceClient,
	videoIDs []uint64,
	retry bool,
) ([]*vpb.VideoInfo, error) {
	var videos []*vpb.VideoInfo
	batches := server.BatchIDs(videoIDs)
	for _, batch := range batches {
		videoResponse, err := videoClient.GetVideo(
			ctx,
			&vpb.GetVideoRequest{VideoIds: batch},
		)
		if err != nil {
			server.videoServiceErrors.Add(1)
			if !retry || server.options.DisableRetry {
				return nil, err
			}
			videoResponse, err = videoClient.GetVideo(
				ctx,
				&vpb.GetVideoRequest{VideoIds: batch},
			)
			if err != nil {
				server.videoServiceErrors.Add(1)
				return nil, err
			}
		}
		videos = append(videos, videoResponse.Videos...)
	}
	return videos, nil
}

func (server *VideoRecServiceServer) Fallback() ([]*vpb.VideoInfo, bool) {
	if server.options.DisableFallback {
		return nil, false
	} else {
		videos := server.cache.Retrieve()
		if videos == nil { // cache not populated
			return nil, false
		} else {
			server.staleResponses.Add(1)
			return videos, true
		}
	}
}

func (server *VideoRecServiceServer) RankAndTruncate(
	userInfo *upb.UserInfo,
	limit int,
	videos []*vpb.VideoInfo,
	stale bool,
) (*pb.GetTopVideosResponse, error) {
	if userInfo != nil {
		var videoRanker ranker.BcryptRanker
		videoMap := make(map[*vpb.VideoInfo]uint64)
		for _, video := range videos {
			videoMap[video] = videoRanker.Rank(userInfo.UserCoefficients, video.VideoCoefficients)
		}
		sort.Slice(videos, func(i, j int) bool {
			return videoMap[videos[i]] > videoMap[videos[j]]
		})
	}
	if limit != 0 && limit < len(videos) {
		videos = videos[:limit]
	}
	return &pb.GetTopVideosResponse{
		Videos:        videos,
		StaleResponse: stale,
	}, nil
}

func (server *VideoRecServiceServer) GetTopVideos(
	ctx context.Context,
	req *pb.GetTopVideosRequest,
) (*pb.GetTopVideosResponse, error) {
	startTime := time.Now()
	server.totalRequests.Add(1)
	server.activeRequests.Add(1)
	defer func() {
		server.activeRequests.Add(^uint64(0)) // decrement by 1
		latency := time.Since(startTime).Milliseconds()
		server.totalLatency.Add(uint64(latency))
		server.latencyDigest.Add(float64(latency), 1)
	}()

	// limit less than 0
	if req.Limit < 0 {
		statusError := status.Errorf(codes.InvalidArgument, "limit must be positive")
		log.Printf("Error occured when calling GetTopVideos: %v", statusError.Error())
		server.totalErrors.Add(1)
		return nil, statusError
	}

	userClient := server.GetUserClientRoundRobin()
	videoClient := server.GetVideoClientRoundRobin()

	// get info for the current user id
	userResponse, err := userClient.GetUser(
		ctx,
		&upb.GetUserRequest{UserIds: []uint64{req.UserId}},
	)
	// retry once for transient errors
	if err != nil && !server.options.DisableRetry {
		server.userServiceErrors.Add(1)
		userResponse, err = userClient.GetUser(
			ctx,
			&upb.GetUserRequest{UserIds: []uint64{req.UserId}},
		)
	}
	if err != nil {
		server.userServiceErrors.Add(1)
		if videos, ok := server.Fallback(); ok {
			return server.RankAndTruncate(nil, int(req.Limit), videos, true)
		} else {
			statusError := status.Errorf(codes.NotFound, "couldn't fetch user info by id %v: %v", req.UserId, err)
			log.Printf("Error occured when calling userClient.GetUser: %v", statusError.Error())
			server.totalErrors.Add(1)
			return nil, statusError
		}
	}

	// check to see if userid is in the returned response
	// barring errors on the server end this should be the case
	var userInfo *upb.UserInfo
	userFound := false
	for _, user := range userResponse.Users {
		if user.UserId == req.UserId {
			userInfo = user
			userFound = true
		}
	}
	if !userFound {
		server.userServiceErrors.Add(1)
		if videos, ok := server.Fallback(); ok {
			return server.RankAndTruncate(nil, int(req.Limit), videos, true)
		} else {
			statusError := status.Errorf(codes.NotFound, "user id not found in user response: %v", err)
			log.Printf("Error occured when finding user id in user info: %v", statusError.Error())
			server.totalErrors.Add(1)
			return nil, statusError
		}
	}

	// query the userClient for subscribed users in batches
	subscribedUsers, err := server.BatchedGetUser(ctx, userClient, userInfo.SubscribedTo, true)
	if err != nil {
		if videos, ok := server.Fallback(); ok {
			return server.RankAndTruncate(userInfo, int(req.Limit), videos, true)
		} else {
			statusError := status.Errorf(codes.NotFound, "couldn't fetch subscribed user(s): %v", err)
			log.Printf("Error occured when calling userClient.GetUser: %v", statusError.Error())
			server.totalErrors.Add(1)
			return nil, statusError
		}
	}

	// get the union of all liked videos, removing any repeats
	likedVideosMap := make(map[uint64]bool)
	var likedVideoIDs []uint64
	for _, user := range subscribedUsers {
		for _, video := range user.LikedVideos {
			if _, ok := likedVideosMap[video]; !ok {
				likedVideosMap[video] = true
				likedVideoIDs = append(likedVideoIDs, video)
			}
		}
	}

	// as before, query the video server in batches
	videos, err := server.BatchedGetVideo(ctx, videoClient, likedVideoIDs, true)
	if err != nil {
		if videos, ok := server.Fallback(); ok {
			return server.RankAndTruncate(userInfo, int(req.Limit), videos, true)
		} else {
			statusError := status.Errorf(codes.NotFound, "couldn't fetch video(s) from video service: %v", err)
			log.Printf("Error occured when calling videoClient.GetVideo: %v", statusError.Error())
			server.totalErrors.Add(1)
			return nil, statusError
		}
	}
	return server.RankAndTruncate(userInfo, int(req.Limit), videos, false)
}

func (server *VideoRecServiceServer) GetStats(
	ctx context.Context,
	req *pb.GetStatsRequest,
) (*pb.GetStatsResponse, error) {
	totalRequests := server.totalRequests.Load()
	totalErrors := server.totalErrors.Load()
	activeRequests := server.activeRequests.Load()
	userServiceErrors := server.userServiceErrors.Load()
	videoServiceErrors := server.videoServiceErrors.Load()
	totalLatency := server.totalLatency.Load()
	p99Latency := float32(server.latencyDigest.Quantile(0.99))
	staleResponses := server.staleResponses.Load()

	// use math/big to prevents overflow as in the case of float32(a) / float32(b)
	var averageLatency float32
	if totalRequests != 0 {
		bigTotalRequests := new(big.Float).SetUint64(totalRequests)
		bigTotalLatency := new(big.Float).SetUint64(totalLatency)
		averageLatency, _ = new(big.Float).Quo(bigTotalLatency, bigTotalRequests).Float32()
	} else {
		averageLatency = float32(math.NaN())
	}

	return &pb.GetStatsResponse{
		TotalRequests:      totalRequests,
		TotalErrors:        totalErrors,
		ActiveRequests:     activeRequests,
		UserServiceErrors:  userServiceErrors,
		VideoServiceErrors: videoServiceErrors,
		AverageLatencyMs:   averageLatency,
		P99LatencyMs:       p99Latency,
		StaleResponses:     staleResponses,
	}, nil
}

func (server *VideoRecServiceServer) ContinuallyRefreshCache() {
	ctx, cancel := context.WithCancel(context.Background())
	server.cancelRefreshCache = cancel
	for {
		select {
		// check if the context has been canceled, in which case end the goroutine
		// doesn't really matter in this assignment because goroutines will exit when the main function ends
		// but might be important if this isn't being called from the main function
		case <-ctx.Done():
			return
		default:
			videoClient := server.GetVideoClientRoundRobin()
			trendingVideoResponse, err := videoClient.GetTrendingVideos(
				ctx,
				&vpb.GetTrendingVideosRequest{},
			)
			// if there is a failure, wait 10s and go back to the beginning
			if err != nil {
				server.videoServiceErrors.Add(1)
				time.Sleep(10 * time.Second)
				continue
			}
			// read in batches
			videos, err := server.BatchedGetVideo(ctx, videoClient, trendingVideoResponse.Videos, false)
			// if there is a failure, wait 10s and go back to the beginning
			if err != nil {
				server.videoServiceErrors.Add(1)
				time.Sleep(10 * time.Second)
				continue
			}
			server.cache.Set(videos)
			time.Sleep(time.Duration(trendingVideoResponse.ExpirationTimeS) * time.Second)
		}
	}
}
