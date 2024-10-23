# CS426: Lab 0 Introduction to Go and Concurrent Programming

This lab is intended to introduce you to the Go programming language
and common concurrency primitives within Go. Concurrency is everywhere
when building distributed systems. Go is a modern programming language,
originally developed at Google, which has several built-in facilities
to manage concurrency.

## Logistics
**Policies**
- Lab 0 is meant to be an **individual** assignment. Please see the [Collaboration and AI Policy](../collaboration_and_ai_policy.md) for details.
- We will help you strategize how to debug but WE WILL NOT DEBUG YOUR CODE FOR YOU.
- Please keep and submit a time log of time spent and major challenges you've encountered. This may be familiar to you if you've taken CS323. See [Time logging](../time_logging.md) for details.

- Questions? Post to Canvas/Ed (preferred) or email the teaching staff.

**Submission deadline: 23:59 ET Wednesday Sep 18, 2024**

**Submission logistics** Submit as a `.tar.gz` archive on Canvas named after your NetID.
E.g., `xs66.tar.gz` which should contain a folder named after you NetID with the following files:

```
  time.log
  discussions.md
  merge_channels.go
  merge_test.go
  parallel_fetcher.go
  parallel_fetcher_test.go
  queue.go
  queue_test.go
  semaphore.go
  semaphore_test.go
```

## Getting Started

### Set up your development environment
Follow the guide [here](../devenv/README.md).

### Introductory reading on Go
Read through the [Tour of Go](https://go.dev/tour/list) for the
basics of programming in Go. Pay particular attention to the sections
on generics, concurrency, and error handling.

Go provides two powerful built-in features for managing concurrency:
 - **goroutines** which are lightweight threads to run tasks concurrently (and in parallel, on a system with multiple processing units)
 - **channels** (or `chan`) which allow for safe communication between goroutines

Go also provides a large amount of concurrency utilities as *library* code. Consider reading
about the following:
 - [`context.Context`](https://pkg.go.dev/context): which allow for calling code to cancel operations (e.g. with timeouts or deadlines)
 and potentially pass "contextual" data
 - [`sync.Mutex`](https://go.dev/tour/concurrency/9): which provides standard mutual exclusion (or "locking") when accessing shared state
 from many threads of execution
 - [`semaphore`](https://pkg.go.dev/golang.org/x/sync/semaphore): which provides traditional [Counting Semaphore](https://en.wikipedia.org/wiki/Semaphore_(programming)) behavior
 - [`sync.WaitGroup`](https://pkg.go.dev/sync#WaitGroup): which helps when waiting for a number of concurrent tasks to complete

## Written Questions (Short Answers)

These questions are meant to test your understanding of the above resources. It may be helpful to complete
these questions and understand the answers before diving into the coding section of this lab.

You should write your answers in your `discussions.md` file. Keep answers brief.

1) What is the difference between an unbuffered channel and a buffered channel?
2) Which is the default in Go? Unbuffered or buffered?
3) What does the following code do?

```go
func FunWithChannels() {
    ch := make(chan string)

    ch <- "hello world!"

    message := <-ch
    fmt.Println(message)
}
```

4) In the function signature of `MergeChannels` in `merge_channels.go`:
```go
// Make sure to read the comment block in merge_channels.go
// T is a generic type in this method signature: https://go.dev/tour/generics/2
func MergeChannels[T any](a <-chan T, b <-chan T, out chan<- T) {
```

What is the difference between `<-chan T`, `chan<- T` and `chan T`?

5) What happens when you read from a closed channel? What about a `nil` channel?
6) When does the following loop terminate?
```go
func FunReadingChannels(ch chan string) {
    for item := range ch {
        fmt.Println(item)
    }
}
```

7) How can you determine if a `context.Context` is done or canceled?
8) What does the following code (most likely) print in the most recent versions of Go (e.g., Go 1.23)? Why is that?
```go
for i := 1; i <= 3; i++ {
    go func() {
        time.Sleep(time.Duration(i) * time.Second)
        fmt.Printf("%d\n", i)
    }()
}
fmt.Println("all done!")
```

9) What concurrency utility might you use to "fix" **question 8**?

10) What is the difference between a mutex (as in `sync.Mutex`) and a semaphore (as in `semaphore.Weighted`)?

11) What does the following code print?
```go
type Bar struct{}
type Foo struct {
	items  []string
	str    string
	num    int
	barPtr *Bar
	bar    Bar
}

func FunWithStructs() {
	var foo Foo
	fmt.Println(foo.items)
	fmt.Println(len(foo.items))
	fmt.Println(foo.items == nil)
	fmt.Println(foo.str)
	fmt.Println(foo.num)
	fmt.Println(foo.barPtr)
	fmt.Println(foo.bar)
}
```

12) What does `struct{}` in the type `chan struct{}` mean? Why might you use it?

13) **Bonus**: What might be different about **question 8** in different versions of Go (assuming after applying your fix in **question 9**)?

## Coding Part A. Merging channels

When dealing with concurrent tasks, you may need to manage multiple channels. For instance,
if you were fetching data from multiple services you may create a goroutine per service
and write the results to a channel to be processed as they are avaiable.

## A1. Implementation of merging channels
For this exercise, you will implement various utilities for merging the results from
concurent sources, starting with merging two channels. Follow the comments
in `merge_channels.go` and implement all three functions according to the instructions.

As you work on each method, run the provided tests with `go test . -v`. To run only the merge tests,
try `go test . -v -run 'TestMerge.*'`, or `go test . -v -run TestMergeChannels` for particular tests.

Your tests may hang if they are not implemented correctly. Correct implementations should run
quickly.

### A2. Unit tests
We have provided a basic set of tests for functionality. **Implement additional unit tests** on
your own in `merge_test.go` (see `TestMergeFetchesAdditional`). These should test something
unique about your implementation and have assertions (using `testing.T` or `require`, as used
in other tests).

## Coding Part B. Concurrency Primitives

Channels operate as queues between communicating processes. In this section, you will build
up your own implementation of an unbounded concurrent queue and sepmahores which are fundamental
building blocks for concurrency.

### B1. Generic and Concurrent Queues
Start in `queue.go`, filling in the necessary sections. Each section should build on the last.
Continue testing your solutions with `go test .` as you implement the various sections.

Once done implementing `ConcurrentQueue[T]`, run tests with the race detector:
```
go test -race . -v -run 'Test.*Queue'
```
Note that the tests with race detector (`-race`) will take longer to run.

### B2. Using Sephamores
Implement `ParallelFetcher` in `parallel_fetcher.go` using the weighted semaphore in the Go standard library.

As you implement `ParallelFetcher`, run tests with the race detector:
```
go test -race . -v -run 'TestFetcher.*'
```

### B3. Building a Semaphore
In the last part of this lab, you will build your own semaphore based on
the native `chan` in Go. Open `semaphore.go` and fill in the implementation. Once done, your
code should pass all tests via `go test -race -v .`.

### B4. Testing
Similar to part A, you must **implement additional unit tests** for your implementations.
See the `TODO` sections in each of the test files.

# End of Lab 0
---

# Go Tips and FAQs
 - After the Tour of Go, use https://go.dev/doc/effective_go and https://gobyexample.com/
 - Use these commands:
    - `go fmt`
    - `go mod tidy` cleans up the module dependencies.
    - `go test -race ...` turns on the Go race detector. Note that `-race` must come before any file name in the command.
    - `go vet` examines Go source code and reports suspicious constructs, such as Printf calls whose arguments do not align with the format string. Vet uses heuristics that do not guarantee all reports are genuine problems, but it can find errors not caught by the compilers.
 - `package testmain: cannot find package` error:
   - Chances are the `GOPATH` env var is not set properly. Run these commands in the terminal: `export GOPATH=$HOME/go` and `export PATH=$PATH:$GOROOT/bin:$GOPATH/bin`. You can also add these to your `~/.bachrc` or `~/.zshrc`.
