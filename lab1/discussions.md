### A1

`NewClient(...)` is similar to `socket()` and `connect()`, because it creates a communication channel and establishes a connection to the gRPC server.


### A2

`NewClient(...)` can fail for the following reasons:

1. if the server is down/otherwise unavailable: `codes.Unavailable`, which is used when a server is unavailable.

2. if the wrong address is used: `codes.InvalidArgument`, because this is to do with malformed input on the client side. 

3. authentication fails: `codes.Unauthenticated`, which is used when the credentials cannot be authenticated and the server refuses the connection.

4. server is overloaded: `codes.ResourceExhausted`, for when some kind of resource is exhausted for the server, like user quota or memory.

5. timeout due to latency: `codes.DeadlineExceeded`, for when the latency is so large that there is a timeout before the server can return anything.

I decided to go with `codes.Unavailable` because it seems like the most widely applicable case, when the server cannot be reached, whereas the other possible codes are more specific to particular errors. It is also a good description of the situation, where `NewClient()` cannot establish a connection to the server.


### A3

First, the `GetUserRequest` object is marshaled into the correct protobuf format and sent in a gRPC request. DNS resolution is done to find the correct node, then the request is sent over the established TCP connection. Then, the server unmarshals the request, processes it, and sends it back in a marshaled protobuf format. Finaly, the client listens for the response and unmarshals it.

At each step, errors can occur: firstly, if the request is malformed, it may not be possible to marshal it into the correct format. Secondly, if DNS resolution fails. Thirdly, if the TCP connection fails for whatever reason. Fourth, if the server goes down, such that it cannot receive the message. Fifth, if the server cannot corectly process the request on its end due to invalid arguments in the request or issues with its database. Sixth, if there is latency such that the request times out. Seventh, if the server runs out of resources and is unable to process the request.

`GetUser()` can return an error even when network calls succeed -- as mentioned above, this can happen if the request is malformed, if invalid arguments are sent (e.g. a nonexistent user), and/or if the server is unable to process it correctly in the alotted time.


### ExtraCredit1

Network errors are typically detected at the transport layer. If the connection fails, or DNS resolution fails, or if the server is unresponsive, the client will receive the error upon making a request. If there is excessive latency such that there is a timeout, this is typically detected through the client's inbuilt timer. 

For errors on the server's end, this can be straightforwardly detected by the server when it attempts to query its database or otherwise process the request.


### A4

It would fail, because a `conn` is tied to a specific address, and if the two services are at different addresses, requests sent to `VideoService` would still be sent to `UserService` (or vice versa, depending on order of creation). Since the server wouldn't be able to handle those requests, they would fail.


### A6

```
2024/09/20 02:35:02 Welcome jl4268! The UserId we picked for you is 203115.

2024/09/20 02:35:02 This user has name Schumm4953, their email is theodorarunte@yundt.org, and their profile URL is https://user-service.localhost/profile/203115
2024/09/20 02:35:03 Recommended videos:
2024/09/20 02:35:03   [0] Video id=1161, title="red Kohlrabi", author=Allan Huel, url=https://video-data.localhost/blob/1161
```


### A8

 Since in this case we don't care about the order of responses returned from the server, I think concurrent requests are a good idea, but there are pros and cons.

The advantage is that, depending on the server, the batches can be concurrently processed, allowing for lower latency. In particular, this can take advantage of multithreading/multiprocessing/distributed architecture on the server backend. Go allows for lightweight concurrency primitives, which makes it relatively simple to implement.

The disadvatage is that this can lead to more complicated error handling -- if one batch fails but another doesn't, we now have to concurrently deal with errors, meaning some sort of state has to be tracked in order to figure out which batches need to be retried and which ones don't.


### ExtraCredit2:

Instead of immediately sending requests to `UserService`/`VideoService`, we can collect requests over a set time interval or until the number of requests reaches the maximum batch size, whichever happens first. This allows us to query the services more efficiently by using the maximum batch size as much as possible. LRU caching can also be used to cache the received data, such that recently queried users or videos can be quickly retrieved without needing to go through the corresponding servers.

```
type VideoRecService struct {
    ...
    requestQueue         chan Request    // queue for incoming requests
    userCache            Cache           // cache for user data
    videoCache           Cache           // cache for video data
}

func HandleRequest(request) {
    requestQueue <- request
}

func CollectRequests() {
    if len(requestQueue) >= maxBatchSize || server timeout {
        return requests from queue equal to batch size
    } else {
        keep waiting
    }
}

func Process() {
    requests := CollectRequests()
    output := []
    for request in requests {
        if request in userCache or videoCache {
            add cached data to output
            remove request from requests
        }
    }
    userIDs := userService(requests)
    videoIDs := videoService(requests)
    cache the received data, removing least recently used data from the caches
    ...
}
```

### B2

```
now_us  total_requests  total_errors    active_requests user_service_errors     video_service_errors    average_latency_ms      p99_latency_ms  stale_responses
1727041501780033        0       0       0       0       0       NaN     NaN     0
1727041502781298        0       0       0       0       0       NaN     NaN     0
1727041503781380        0       0       0       0       0       NaN     NaN     0
1727041504781807        18      0       2       0       0       261.56  480.00  0
1727041505781236        28      0       1       0       0       229.82  480.00  0
1727041506781874        38      0       2       0       0       205.08  480.00  0
1727041507784792        48      0       4       0       0       203.94  480.00  0
1727041508781794        58      0       2       0       0       231.83  479.16  0
1727041509782841        68      0       3       0       0       228.09  477.90  0
1727041510781030        78      0       1       0       0       227.09  476.22  0
1727041511782078        88      0       3       0       0       218.01  475.10  0
1727041512781246        98      0       2       0       0       219.72  473.56  0
1727041513781675        108     0       3       0       0       219.16  472.30  0
1727041514782320        118     0       2       0       0       219.30  470.76  0
```

### C1

Retrying is bad if the request isn't idempotent, e.g. monetary transactions. In that case, retrying a failed request might mean accidentally executing the same operation twice, if the request returns a failure but is actually still being processed. 

In addition, if the service has strict time limits, e.g. for time-sensitive applications, retrying can add to the overall latency and exceed the required response time limits. 

Also, if the failure is due to non-transient errors, e.g. invalid arguments, retrying will only waste computational resources. A solution to this is to check the code and only retry when it is likely to be a transient error.

Last but not least, if retries have already been implemented elsewhere, e.g. in the server, retrying again on the client can lead to logic errors and potentially cause infinite retry loops.


### C2

The benefit of returning the expired responses is that users are still able to get recommendations, even though the data may be outdated. This means that the system can still (sort of) function when the servers it relies on fail, but the disadvantage is that the videos it serves may no longer be trending, leading to irrelevant responses from the server. The longer this goes on, the more stale the responses are going to be.

The benefit of returning an error is that the user is immediately made aware of the issue, which lets them employ their own error protocol/contingencies. For example, the user may wish to query again through a different server/channel. The disadvantage is that this means the video recommendation server will fail more frequently than it would otherwise have, and users might retry frequently, which increases load.

In cases where some data is better than no data at all, and where service downtime is expected to be short, returning expired responses may be a better solution. On the other hand, if providing accurate data is critical, returning an error is probably better. In this case, trending videos are (probably) not critical information, and the user likely just wants to get a list of videos that they will potentially be interested in, so returning the expired responses is a better solution in my opinion.


### C3

One thing I could do is to implement retrying with exponential backoff in the event of a failure, i.e. multiplicatively decrease the rate of retrying, before falling back to the cache. The advantage of this is that it allows the system to better recover from transient errors, whereas the retry-once method is much more limited and fails if the error lasts even slightly longer beyond the retry. This can help minimize disruptions in the case of temporary errors and help return accurate recommended videos.

The tradeoff is that this can delay the response if retries take too long, which means latency is longer, and it can add to the user/video server load.


### C4

There is an overhead to establishing a gRPC connection, because it involves multiple round-trip communications, so doing this for every request is going to unnecessarily increase latency. Each connection is going to take up unncessary computing time and resources. For a high throughput service, it will slow down requests because instead of establishing the connection once and amortizing the cost across all the future requests, this connection overhead will happen every single time.

An alternative is to establish the connection once when the server is created, and reuse it for each request. The tradeoff is that it prevents load balancing from efficiently directing requests to appropriate destinations, because all requests are being sent to the same address over the same connection. This can lead to a heavier load on the target server and prevent load balancing from dynamically shifting the load somewhere else.
