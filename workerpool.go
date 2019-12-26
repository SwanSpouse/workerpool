package workerpool

import (
	"github.com/gammazero/deque"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// This value is the size of the queue that workers register their
	// availability to the dispatcher.  There may be hundreds of workers, but
	// only a small channel is needed to register some of the workers.
	readyQueueSize = 16

	// If worker pool receives no new work for this period of time, then stop
	// a worker goroutine.
	idleTimeoutSec = 5
)

// New creates and starts a pool of worker goroutines.
//
// The maxWorkers parameter specifies the maximum number of workers that will
// execute tasks concurrently.  After each timeout period, a worker goroutine
// is stopped until there are no remaining workers.
func New(maxWorkers int) *WorkerPool {
	// There must be at least one worker.
	// 最少一个worker；最少一个并发度，串行
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	pool := &WorkerPool{
		taskQueue:    make(chan func(), 1),                   // 任务队列
		maxWorkers:   maxWorkers,                             // 最多有多少个工作的worker
		readyWorkers: make(chan chan func(), readyQueueSize), // 准备好的worker队列
		timeout:      time.Second * idleTimeoutSec,
		stoppedChan:  make(chan struct{}),
	}

	// Start the task dispatcher.
	// 分发任务
	go pool.dispatch()

	return pool
}

// WorkerPool is a collection of goroutines, where the number of concurrent
// goroutines processing requests does not exceed the specified maximum.
type WorkerPool struct {
	maxWorkers   int              // 最大的并发数
	timeout      time.Duration    // 超时时间 每隔这么长时间去检查一下是否有空闲的worker
	taskQueue    chan func()      // 任务队列
	readyWorkers chan chan func() // 空闲的worker队列
	stoppedChan  chan struct{}    // workerPool的停止标记
	waitingQueue deque.Deque      // task的waiting队列 taskQueue中的任务若没有worker来接受，则会跑到这里
	stopOnce     sync.Once        // 保证workerPool只Stop一次
	stopped      int32            // stop 标志
	waiting      int32            // waiting 标志
}

// Stop stops the worker pool and waits for only currently running tasks to
// complete.  Pending tasks that are not currently running are abandoned.
// Tasks must not be submitted to the worker pool after calling stop.
//
// Since creating the worker pool starts at least one goroutine, for the
// dispatcher, Stop() or StopWait() should be called when the worker pool is no
// longer needed.
// 停止不等待
func (p *WorkerPool) Stop() {
	p.stop(false)
}

// StopWait stops the worker pool and waits for all queued tasks tasks to
// complete.  No additional tasks may be submitted, but all pending tasks are
// executed by workers before this function returns.
// 停止且等待所有的task执行完成
func (p *WorkerPool) StopWait() {
	p.stop(true)
}

// Stopped returns true if this worker pool has been stopped.
// 判断当前的worker是否被关闭了
func (p *WorkerPool) Stopped() bool {
	return atomic.LoadInt32(&p.stopped) != 0
}

// Submit enqueues a function for a worker to execute.
//
// Any external values needed by the task function must be captured in a
// closure.  Any return values should be returned over a channel that is
// captured in the task function closure.
//
// Submit will not block regardless of the number of tasks submitted.  Each
// task is immediately given to an available worker or passed to a goroutine to
// be given to the next available worker.  If there are no available workers,
// the dispatcher adds a worker, until the maximum number of workers are
// running.
//
// After the maximum number of workers are running, and no workers are
// available, incoming tasks are put onto a queue and will be executed as
// workers become available.
//
// When no new tasks have been submitted for a time period and a worker is
// available, the worker is shutdown.  As long as no new tasks arrive, one
// available worker is shutdown each time period until there are no more idle
// workers.  Since the time to start new goroutines is not significant, there
// is no need to retain idle workers.
func (p *WorkerPool) Submit(task func()) {
	// 提交task；这里保证了task不能为空；
	if task != nil {
		p.taskQueue <- task
	}
}

// SubmitWait enqueues the given function and waits for it to be executed.
// 还能等待特定的任务执行呢
func (p *WorkerPool) SubmitWait(task func()) {
	if task == nil {
		return
	}
	doneChan := make(chan struct{})
	p.taskQueue <- func() {
		task()
		close(doneChan)
	}
	<-doneChan
}

// WaitingQueueSize will return the size of the waiting queue
func (p *WorkerPool) WaitingQueueSize() int {
	// 查看有多少个正在等待worker的task
	return int(atomic.LoadInt32(&p.waiting))
}

// dispatch sends the next queued task to an available worker.
// dispatch方法是串行的；这里不会有并发的问题发生。
func (p *WorkerPool) dispatch() {
	// 确保最后stoppedChan可以被关闭；这里关闭了之后会触发解除最后的阻塞
	defer close(p.stoppedChan)
	timeout := time.NewTimer(p.timeout) // 创建一个计时器
	var (
		workerCount    int
		task           func()
		ok, wait       bool
		workerTaskChan chan func()
	)
	startReady := make(chan chan func())
Loop: // 记住这里是Loop
	for {
		// As long as tasks are in the waiting queue, remove and execute these
		// tasks as workers become available, and place new incoming tasks on
		// the queue.  Once the queue is empty, then go back to submitting
		// incoming tasks directly to available workers.
		if p.waitingQueue.Len() != 0 {
			select {
			case task, ok = <-p.taskQueue:
				if !ok {
					break Loop
				}
				if task == nil {
					wait = true
					break Loop
				}
				p.waitingQueue.PushBack(task)
			case workerTaskChan = <-p.readyWorkers:
				// A worker is ready, so give task to worker.
				workerTaskChan <- p.waitingQueue.PopFront().(func())
			}
			atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.Len()))
			continue
		}
		// 每次在这里重置超时时间
		timeout.Reset(p.timeout)
		// 这里两个case之间没有先后顺序的问题；
		select {
		// 在这里可以收到task说明有源源不断的任务进来
		case task, ok = <-p.taskQueue:
			if !ok || task == nil {
				break Loop
			}
			// Got a task to do.
			select {
			// 有空闲的worker
			case workerTaskChan = <-p.readyWorkers:
				// A worker is ready, so give task to worker.
				// 把task交给worker来进行处理
				workerTaskChan <- task
			default:
				// No workers ready.
				// Create a new worker, if not at max.
				// 没有找到空闲的worker；如果worker数量没有达到maxWorkers，则会创建新的worker
				if workerCount < p.maxWorkers {
					// 记录当前worker的数量
					workerCount++
					go func(t func()) {
						//  启动worker
						startWorker(startReady, p.readyWorkers)
						// Submit the task when the new worker.
						// 新worker的任务队列；
						taskChan := <-startReady
						// 将当前任务t交给新创建的worker来进行处理
						taskChan <- t
					}(task)
				} else {
					// Enqueue task to be executed by next available worker.
					// 如果当前worker的数量已经达到maxWorkers；说明并发度已经到了，这个时候就要进waitingQueue了
					p.waitingQueue.PushBack(task)
					// 同时waiting的数量增加
					atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.Len()))
				}
			}
		case <-timeout.C:
			// Timed out waiting for work to arrive.  Kill a ready worker.
			// 这里有一个计时器，到这里说明，到这里说明已经没有那么多的task可以被执行了。
			if workerCount > 0 {
				select {
				// 如果这时候有空闲的worker，则进行关闭；回收资源
				case workerTaskChan = <-p.readyWorkers:
					// A worker is ready, so kill.
					close(workerTaskChan)
					workerCount--
				default:
					// 到这里说明所有的worker都很忙；
					// No work, but no ready workers.  All workers are busy.
				}
			}
		}
	}

	// If instructed to wait for all queued tasks, then remove from queue and
	// give to workers until queue is empty.
	// 能到这里说明taskQueue里面已经没有任务了；说明要么所有的任务都执行完了，要么所有的任务都在执行中。
	if wait {
		// 等待所有的任务执行完成
		// WaitingQueue里面还有task，说明所有的worker都在工作，同时还有任务没有被执行
		for p.waitingQueue.Len() != 0 {
			// 看看有没有worker空闲出来
			workerTaskChan = <-p.readyWorkers
			// A worker is ready, so give task to worker.
			// 如果有worker空闲出来，则给它从waitingQueue里面拿一个任务出来
			workerTaskChan <- p.waitingQueue.PopFront().(func())
			// 重新计算当前waitingQueue的长度
			atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.Len()))
		}
	}

	// Stop all remaining workers as they become ready.
	// 每当有一个worker空闲下来的时候，就关闭一个
	for workerCount > 0 {
		workerTaskChan = <-p.readyWorkers
		close(workerTaskChan)
		workerCount--
	}
}

// startWorker starts a goroutine that executes tasks given by the dispatcher.
//
// When a new worker starts, it registers its availability on the startReady
// channel.  This ensures that the goroutine associated with starting the
// worker gets to use the worker to execute its task.  Otherwise, the main
// dispatcher loop could steal the new worker and not know to start up another
// worker for the waiting goroutine.  The task would then have to wait for
// another existing worker to become available, even though capacity is
// available to start additional workers.
//
// A worker registers that is it available to do work by putting its task
// channel on the readyWorkers channel.  The dispatcher reads a worker's task
// channel from the readyWorkers channel, and writes a task to the worker over
// the worker's task channel.  To stop a worker, the dispatcher closes a
// worker's task channel, instead of writing a task to it.
func startWorker(startReady, readyWorkers chan chan func()) {
	go func() {
		taskChan := make(chan func())
		var task func()
		var ok bool
		// Register availability on starReady channel.
		// 把自己注册出去，让别人可以往taskChan里面提交任务
		startReady <- taskChan
		for {
			// 自己也开始从taskChan里面接受任务
			// Read task from dispatcher.
			task, ok = <-taskChan
			if !ok {
				// 说明task已经关闭了 那么自己也退出了
				// Dispatcher has told worker to stop.
				break
			}
			// Execute the task.
			// 在这里执行task TODO @limingji 这里不应该捕获一下panic吗？否则的话panic会导致当前的这个worker xxxx
			task()

			// Register availability on readyWorkers channel.
			// 执行完一个任务之后，把自己重新交还给readyWorkers
			readyWorkers <- taskChan
		}
	}()
}

// stop tells the dispatcher to exit, and whether or not to complete queued
// tasks.
func (p *WorkerPool) stop(wait bool) {
	// 保证WorkPool只会执行一次
	p.stopOnce.Do(func() {
		// 标记当前的worker已经被关闭了
		atomic.StoreInt32(&p.stopped, 1)
		if wait {
			// 噢噢噢噢；这个比较巧妙；用一个这东西标记最有一个任务
			p.taskQueue <- nil
		}
		// Close task queue and wait for currently running tasks to finish.
		// 关闭了之后就肯定不会有新的task了
		close(p.taskQueue)
		<-p.stoppedChan
	})
}
