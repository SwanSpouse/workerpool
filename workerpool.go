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
	// 如果这么长时间没有收到任务来执行；则关闭一个worker
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
	taskQueue    chan func()      // 任务队列 没有缓冲区
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
		// 如果taskQueue满了的话会阻塞在这里
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
	// 如果taskQueue满了的话会阻塞在这里
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
		workerCount    int    // 当前的worker数量
		task           func() // task
		ok, wait       bool
		workerTaskChan chan func() // worker的task队列
	)
	// 这个是没有缓冲区的；会确保一个worker启动起来
	startReady := make(chan chan func())
Loop: // 记住这里是Loop
	for {
		// As long as tasks are in the waiting queue, remove and execute these
		// tasks as workers become available, and place new incoming tasks on
		// the queue.  Once the queue is empty, then go back to submitting
		// incoming tasks directly to available workers.
		// 只要waiting queue里面有task，每当worker空闲下来的时候就去执行这些task
		// 同时把新来的task塞到waitingQueue里面。
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
				// 把task塞到waiting queue里面等待执行
				p.waitingQueue.PushBack(task)
			case workerTaskChan = <-p.readyWorkers:
				// A worker is ready, so give task to worker.
				// 有worker腾出时间来处理任务；把队列最前面的func交给它
				workerTaskChan <- p.waitingQueue.PopFront().(func())
			}
			// 这里不太理解为啥要维护一个waiting这样一个数字呢？用的时候直接拿不是更好
			// 维护这个数字的成本不高，但是用的时候直接拿会有并发的问题？
			atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.Len()))
			continue
		}
		// 如果waitingQueue的Len不为空，则不会走到这里来；因为这里到时间了就开始销毁worker了
		// 每次在这里重置超时时间
		// 这里的timeout记录的是通过多长时间才从 taskQueue中获取到了一个task
		timeout.Reset(p.timeout)
		// 这里两个case之间没有先后顺序的问题；
		select {
		// 在这里可以收到task说明有源源不断的任务进来
		case task, ok = <-p.taskQueue:
			// 在插入task的时候就已经保证了task不为nil；所以task = nil是一个停止信号
			if !ok || task == nil {
				break Loop
			}
			// Got a task to do.
			select {
			// 从这里接收空闲的worker
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
					// 每一个worker都对应着一个goroutine
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
			// 这里有一个计时器，到这里说明，到这里说明已经没有那么多的task可以被执行了。开始缩容了。
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
	// 能到这里说明taskQueue里面已经没有任务了，任务要么在执行过程中，要么处于waitingQueue中
	if wait {
		// 等待所有的任务执行完成
		// WaitingQueue里面还有task，说明所有的worker都在工作，同时还有任务没有被执行
		// 在这里要把所有waitingQueue中的任务也执行掉
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
// 这个方法的精髓；startReady readyWorkers 这俩是放队列的队列；也就是这俩其实是放的worker
// startReady表明启动成功，准备好接收第一个task
// 执行完任务之后，将自己的taskChan交给readyWorkers；表明自己已经做好准备接收以一波任务
// TODO @limingji 这俩chan chan fun()有没有机会整合成为一个
func startWorker(startReady, readyWorkers chan chan func()) {
	go func() {
		taskChan := make(chan func())
		var task func()
		var ok bool
		// Register availability on starReady channel.
		// 把自己注册出去，让别人可以往taskChan里面提交任务
		startReady <- taskChan
		// worker的loop
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
			// 在这里执行task
			// TODO @limingji 这里不应该捕获一下panic吗？否则的话panic会导致当前的这个worker xxxx
			task()

			// Register availability on readyWorkers channel.
			// 执行完一个任务之后，把自己重新交还给readyWorkers，表示自己可以开始接受新任务
			// 这个readyWorkers是有容量限制的；有可能阻塞在这里；
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
			// 噢噢噢噢；这个比较巧妙；用一个这东西标记最后一个任务
			// 而且这个是阻塞队列，nil能写进去了之后说明一定有人接到了这个nil；taskQueue可以关闭了
			p.taskQueue <- nil
		}
		// Close task queue and wait for currently running tasks to finish.
		// 关闭了之后就肯定不会有新的task了
		close(p.taskQueue)
		<-p.stoppedChan
	})
}
