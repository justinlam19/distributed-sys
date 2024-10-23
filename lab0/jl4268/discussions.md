1. An unbuffered channel, like its name suggests, doesn't have a buffer, so sending a value through it will block until it is received. On the other hand, a buffered channel is specified with a capacity N, and it will block only if the buffer is empty or full. For instance, an unbuffered channel only blocks when it has an unreceived value, while a buffered channel with a capacity of 1 will block when it is either empty or full (i.e. regardless of whether there is a value).

2. Go defaults to unbuffered channels.

3. It creates an unbuffered channel `ch` and sends `"hello world!"` to the channel. The string is then received and assigned to the variable `message`, which is printed as `"hello world!"`.

4. `<-chan T` can only be used to receive values of type `T`, i.e. it is read-only. `chan<- T` can only be used to send values of type `T`, i.e. it is write-only. `chan T` can be used for both reading and writing values of type `T`.

5. Reading from a closed channel succeeds and immediately returns the zero value of the channel's underlying type. On the other hand, reading from a `nil` channel will block forever.

6. The loop terminates when the channel `ch` is closed. 

7. We can check the done channel, e.g. `ctx.Done()`, which is closed when the context is done or canceled. This is often used in select-case, where one case is `case <-ctx.Done()`, which executes the relevant code when the `ctx.Done()` channel is closed.

8. In most cases it just prints `"all done!"`. The main thread doesn't wait for the goroutines to complete, and since they have a sleep statement, most of the time the main thread will print `"all done!"` and exit before the goroutines print anything.

9. `sync.WaitGroup` can be used to block the main thread until the goroutines finish.

10. Typically there is a queue to access the semaphore, while typical mutexes don't have a queue, and the unlocker doesn't signal the waiting goroutines. For this Go-specific case, the mutex only allows one goroutine at a time to access the critical section (disregarding something like `RWMutex`), while the semaphore allows up to N goroutines at a time, N being a predefined limit.

11. For `foo.items` it prints the zero value `[]`. Its length is printed as `0`, and the boolean `foo.items == nil` is `true`. `foo.str` is the zero value `""`, i.e. nothing is printed. Then `foo.num` is printed as the zero value `0`. `foo.barPtr` is printed as `<nil>`, i.e. the nil pointer, and finally `foo.bar`is printed as the empty struct `{}`. Therefore, on the whole, the following is printed:
```
[]
0
true

0
<nil>
{}
```
12. `struct{}` means the empty struct. Since the empty struct requires no memory, it can save memory when we only need the channel for notifying/signalling and not to pass messages.

13. In versions predating Go 1.22, this would print:
```
3
3
3
all done!
```
This is because the loop variable had per-loop scope, and since the goroutines copy the variable by reference, the incrementing of the loop variable would affect all the goroutines, such that `i` is printed as `3` for all 3 goroutines.
In contrast, Go 1.22 and later fixes this problem, so the loop variable has per-iteration scope and the expected output is printed:
```
1
2
3
all done!
```
