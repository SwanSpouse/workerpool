package workerpool

import (
	"sync"
	"testing"
	"time"
)

const max = 20

// 测试业务逻辑，保证所有的task都执行了
func TestExample(t *testing.T) {
	wp := New(2)
	requests := []string{"alpha", "beta", "gamma", "delta", "epsilon"}

	rspChan := make(chan string, len(requests))
	for _, r := range requests {
		r := r
		wp.Submit(func() {
			rspChan <- r
		})
	}
	// 等待所有的任务执行完成
	wp.StopWait()

	close(rspChan)
	// 把结果塞到map里面
	rspSet := map[string]struct{}{}
	for rsp := range rspChan {
		rspSet[rsp] = struct{}{}
	}
	// 比较请求和返回值的个数
	if len(rspSet) < len(requests) {
		t.Fatal("Did not handle all requests")
	}
	// 确保每个请求都被处理了
	for _, req := range requests {
		if _, ok := rspSet[req]; !ok {
			t.Fatal("Missing expected values:", req)
		}
	}
}

// 测试最大Worker数量
func TestMaxWorkers(t *testing.T) {
	// 表明当前测试可以和其他测试并行处理
	t.Parallel()
	// 验证maxWorkers数值有效
	wp := New(0)
	if wp.maxWorkers != 1 {
		t.Fatal("should have created one worker")
	}
	// 指定20个worker
	wp = New(max)
	defer wp.Stop()
	// 20个的队列，每启动一个往里面塞一个
	started := make(chan struct{}, max)
	// 一个阻塞队列
	sync := make(chan struct{})

	// Start workers, and have them all wait on a channel before completing.
	// 启动20个worker
	for i := 0; i < max; i++ {
		// 依次提交任务
		wp.Submit(func() {
			// 标明任务已经启动
			started <- struct{}{}
			// sync没有人消费，会阻塞在这里
			<-sync
		})
	}

	// Wait for all queued tasks to be dispatched to workers.
	// 在这里等待5s，确保所有的任务都被分发完
	timeout := time.After(5 * time.Second)
	// 比较一下等待队列的len和WaitingQueueSize；这俩没有理由不相等
	//  TODO @limingji 这里的比较不合理；至少和下面的panic是不符的
	if wp.waitingQueue.Len() != wp.WaitingQueueSize() {
		t.Fatal("Working Queue size returned should not be 0")
		panic("WRONG")
	}
	// 从started依次往外取数据
	for startCount := 0; startCount < max; {
		select {
		case <-started:
			// 从started里面拿数据
			startCount++
		case <-timeout:
			// 如果超时说明这里有问题
			t.Fatal("timed out waiting for workers to start")
		}
	}
	// 这时所有的worker都卡在 <-sync这里
	// Release workers.
	close(sync)
}

// 测试worker是否能够进行复用
func TestReuseWorkers(t *testing.T) {
	t.Parallel()
	// 最大并发数量是5
	wp := New(5)
	defer wp.Stop()

	sync := make(chan struct{})

	// Cause worker to be created, and available for reuse before next task.
	// 提交10个task
	// 这样提交，sleep 100ms 保证任务执行；这样其实相当于顺序执行；只需要一个worker就够了；
	for i := 0; i < 10; i++ {
		// 提交之后就阻塞住了; 在等待数据
		wp.Submit(func() { <-sync })
		// 然后在这里写入数据
		sync <- struct{}{}
		time.Sleep(100 * time.Millisecond)
	}

	// If the same worker was always reused, then only one worker would have
	// been created and there should only be one ready.
	// 这里计算一下当前有多少个处于ready状态的worker
	if countReady(wp) > 1 {
		// 如果ready的worker数量大于1；说明worker没有被重用
		t.Fatal("Worker not reused")
	}
}

// 测试超时机制是否好使
func TestWorkerTimeout(t *testing.T) {
	t.Parallel()
	// 最大并发数是20
	wp := New(max)
	defer wp.Stop()

	sync := make(chan struct{})
	started := make(chan struct{}, max)
	// Cause workers to be created.  Workers wait on channel, keeping them busy
	// and causing the worker pool to create more.
	// 提交20个task 同时让他们都阻塞在 <- sync
	for i := 0; i < max; i++ {
		wp.Submit(func() {
			started <- struct{}{}
			<-sync
		})
	}

	// Wait for tasks to start.
	// 在这里依次startedChan中的数据进行消费
	for i := 0; i < max; i++ {
		<-started
	}
	// 这个时候所有的worker都应该处于阻塞状态；不应该有readyWorker
	if anyReady(wp) {
		t.Fatal("number of ready workers should be zero")
	}
	// Release workers.
	// 这个一执行，所有阻塞的task都释放了
	close(sync)

	// 处于ready状态的worker应该和max相等
	// 这里最高sleep 5s；
	// TODO @limingji 这里有问题，为啥所有的都会走到timeout 5s里面
	if countReady(wp) != max {
		t.Fatal("Expected", max, "ready workers")
	}

	// Check that a worker timed out.
	// sleep多1s；确保空闲的worker被消灭
	// 这里sleep 6s
	time.Sleep((idleTimeoutSec + 1) * time.Second)
	// 这里最高sleep 5s；
	if countReady(wp) != max-1 {
		t.Fatal("First worker did not timeout")
	}

	// Check that another worker timed out.
	// sleep多1s；确保又一个空闲的worker被消灭
	// 这里sleep 6s
	time.Sleep((idleTimeoutSec + 1) * time.Second)
	// 这里最高sleep 5s；
	if countReady(wp) != max-2 {
		t.Fatal("Second worker did not timeout")
	}
}

// 测试Stop
func TestStop(t *testing.T) {
	t.Parallel()

	// 首先创造一个20个worker的池子
	wp := New(max)
	defer wp.Stop()

	// 和上面同样的套路；利用一个20的队列标记开始；
	started := make(chan struct{}, max)
	// 用一个阻塞队列标识停止
	sync := make(chan struct{})

	// Start workers, and have them all wait on a channel before completing.
	// 依次进行启动
	for i := 0; i < max; i++ {
		wp.Submit(func() {
			// 写入队列表明启动；
			started <- struct{}{}
			// 想写入队列被阻塞
			<-sync
		})
	}
	// 5s之后超时
	// Wait for all queued tasks to be dispatched to workers.
	timeout := time.After(5 * time.Second)
	for startCount := 0; startCount < max; {
		select {
		case <-started:
			// 在这里记录已经启动了的数量
			startCount++
		case <-timeout:
			t.Fatal("timed out waiting for workers to start")
		}
	}
	// Release workers.
	// 关闭；让所有的task 执行成功；
	close(sync)
	// 判断是否停止
	if wp.Stopped() {
		// TODO @limingji 感觉这里应该是t.Fatal 下面的也是
		t.Error("pool should not be stopped")
	}
	// 停止 这里只是不允许任务继续写入了
	wp.Stop()
	// 其实感觉把这个判断挪到wp.Stop上面，也不会有问题；close之后立刻就没有worker
	if anyReady(wp) {
		t.Error("should have zero workers after stop")
	}
	// 这里应该已经被认定为停止
	if !wp.Stopped() {
		t.Error("pool should be stopped")
	}

	// Start workers, and have them all wait on a channel before completing.
	// 重新启动一次
	wp = New(5)
	sync = make(chan struct{})
	finished := make(chan struct{}, max)
	for i := 0; i < max; i++ {
		wp.Submit(func() {
			// 这回是先阻塞了
			<-sync
			finished <- struct{}{}
		})
	}
	// Call Stop() and see that only the already running tasks were completed.
	// 10s 之后stop
	go func() {
		// TODO 这里Sleep之后下面还会有问题吗？
		time.Sleep(10000 * time.Millisecond)
		close(sync)
	}()
	// 这里先stop了；stop不会影响里面的worker只是说明不会再有新的task了
	wp.Stop()
	var count int
Count:
	for count < max {
		select {
		case <-finished:
			count++
		default:
			break Count
		}
	}
	// TODO 感觉这里不可能超过5；毕竟Sleep 10s才关闭；超过1都不太可能
	if count > 5 {
		t.Error("Should not have completed any queued tasks, did", count)
	}
	// TODO @limingji 拼写错了
	// Check that calling Stop() againg is OK.
	wp.Stop()
}

// 测试StopWait
func TestStopWait(t *testing.T) {
	t.Parallel()

	// Start workers, and have them all wait on a channel before completing.
	wp := New(5)
	sync := make(chan struct{})
	finished := make(chan struct{}, max)
	for i := 0; i < max; i++ {
		wp.Submit(func() {
			<-sync
			finished <- struct{}{}
		})
	}

	// Call StopWait() and see that all tasks were completed.
	// 在这里Sleep 10ms
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(sync)
	}()
	// 会在这里卡住 等待上面的close，然后往下走；
	wp.StopWait()
	// 应该所有的task都执行完了
	for count := 0; count < max; count++ {
		select {
		case <-finished:
		default:
			t.Error("Should have completed all queued tasks")
		}
	}
	// 因为已经stop了 然后不能有ready的worker
	if anyReady(wp) {
		t.Error("should have zero workers after stopwait")
	}
	// 验证停止的标志
	if !wp.Stopped() {
		t.Error("pool should be stopped")
	}

	// Make sure that calling StopWait() with no queued tasks is OK.
	// 直接Stop也不会有问题
	wp = New(5)
	wp.StopWait()

	// 也不会有ready的Worker
	if anyReady(wp) {
		t.Error("should have zero workers after stopwait")
	}

	// TODO @limingji 拼写错了
	// Check that calling StopWait() againg is OK.
	wp.StopWait()
}

// 测试提交并等待
func TestSubmitWait(t *testing.T) {
	wp := New(1)
	defer wp.Stop()

	// Check that these are noop.
	// 是无法提交nil的任务的；
	wp.Submit(nil)
	wp.SubmitWait(nil)

	done1 := make(chan struct{})
	// 提交任务；先sleep
	wp.Submit(func() {
		time.Sleep(100 * time.Millisecond)
		close(done1)
	})
	select {
	case <-done1:
		// 不应该走到这里来；因为wp.Submit不阻塞
		t.Fatal("Submit did not return immediately")
	default:
	}

	done2 := make(chan struct{})
	// 这个地方就会阻塞了；会等待任务执行完成
	wp.SubmitWait(func() {
		time.Sleep(100 * time.Millisecond)
		close(done2)
	})
	select {
	case <-done2:
	default:
		t.Fatal("SubmitWait did not wait for function to execute")
	}
}

// 测试超限
func TestOverflow(t *testing.T) {
	wp := New(2)
	releaseChan := make(chan struct{})

	// Start workers, and have them all wait on a channel before completing.
	// 直接提交了64个task 等待从releaseChan中消费数据
	for i := 0; i < 64; i++ {
		wp.Submit(func() { <-releaseChan })
	}

	// Start a goroutine to free the workers after calling stop.  This way
	// the dispatcher can exit, then when this goroutine runs, the workerpool
	// can exit.
	go func() {
		// 在这里等待1ms;然后关闭releaseChan
		<-time.After(time.Millisecond)
		close(releaseChan)
	}()
	// Stop了之后就不会再执行任何的task了
	wp.Stop()

	// Now that the worker pool has exited, it is safe to inspect its waiting
	// queue without causing a race.
	// 所以现在waitingQueue里面剩下的肯定就是64-2个
	qlen := wp.waitingQueue.Len()
	if qlen != 62 {
		t.Fatal("Expected 62 tasks in waiting queue, have", qlen)
	}
}

// 测试竞争
func TestStopRace(t *testing.T) {
	wp := New(20)
	releaseChan := make(chan struct{})
	workRelChan := make(chan struct{})

	// Start workers, and have them all wait on a channel before completing.
	for i := 0; i < 20; i++ {
		//  提交了20个task
		wp.Submit(func() { <-workRelChan })
	}

	// Sleep 5s 这样每一个worker都能分配到一个task；然后卡住
	time.Sleep(5 * time.Second)
	for i := 0; i < 64; i++ {
		go func() {
			// 这64个worker都会卡住在这里
			<-releaseChan
			wp.Stop()
		}()
	}
	// 在这里把workRelChan关闭；20个task都执行
	close(workRelChan)
	// 这里把releaseChan关闭；没太理解这个测试在测试啥
	close(releaseChan)
}

// Run this test with race detector to test that using WaitingQueueSize has no
// race condition
func TestWaitingQueueSizeRace(t *testing.T) {
	const (
		goroutines = 10
		tasks      = 20
		workers    = 5
	)
	wp := New(workers)
	maxChan := make(chan int)
	// 在这里启动10个goroutine
	for g := 0; g < goroutines; g++ {
		go func() {
			max := 0
			// Submit 100 tasks, checking waiting queue size each time.  Report
			// the maximum queue size seen.
			// 每个goroutine启动20个task
			for i := 0; i < tasks; i++ {
				// 就在这里面sleep 1ms
				wp.Submit(func() {
					time.Sleep(time.Microsecond)
				})
				// 获取等待队列的大小
				waiting := wp.WaitingQueueSize()
				if waiting > max {
					max = waiting
				}
			}
			// max 是这个goroutine 这20次循环里面能看到的最大的等待队列的长度
			maxChan <- max
		}()
	}

	// Find maximum queuesize seen by any thread.
	maxMax := 0
	for g := 0; g < goroutines; g++ {
		max := <-maxChan
		if max > maxMax {
			maxMax = max
		}
	}
	// 如果maxMax == 0 不合理；咋可能没有等待的呢
	if maxMax == 0 {
		t.Error("expected to see waiting queue size > 0")
	}
	// 这个也不合理；它居然可以看到上限
	if maxMax >= goroutines*tasks {
		t.Error("should not have seen all tasks on waiting queue")
	}
}

// 判断当前是否存在readyWorker 这是个时间点的判断；只判断一瞬间
func anyReady(w *WorkerPool) bool {
	select {
	case wkCh := <-w.readyWorkers:
		w.readyWorkers <- wkCh
		return true
	default:
	}
	return false
}

// 查看是否有处于ready状态的worker
func countReady(w *WorkerPool) int {
	// Try to pull max workers off of ready queue.
	// 超时 TODO @limingji 这里为啥要统计5s内的readyWorker呢？ 不也是应该统计一瞬间的吗？
	timeout := time.After(5 * time.Second)
	readyTmp := make(chan chan func(), max)
	var readyCount int
	for i := 0; i < max; i++ {
		select {
		case wkCh := <-w.readyWorkers:
			readyTmp <- wkCh
			// readyWorker计数
			readyCount++
			// 超时了
		case <-timeout:
			// 当前记录了多少个readyWorker
			readyCount = i
			// 跳出循环
			i = max
		}
	}

	// Restore ready workers.
	// 为了不能够影响worker的正常工作；把worker从readyWorker里面拿出来之后要放回去
	close(readyTmp)
	go func() {
		// 拿出来的要放回去
		for r := range readyTmp {
			w.readyWorkers <- r
		}
	}()
	return readyCount
}

/*

Run benchmarking with: go test -bench '.'

*/

func BenchmarkEnqueue(b *testing.B) {
	wp := New(1)
	defer wp.Stop()
	releaseChan := make(chan struct{})

	b.ResetTimer()

	// Start workers, and have them all wait on a channel before completing.
	for i := 0; i < b.N; i++ {
		wp.Submit(func() { <-releaseChan })
	}
	close(releaseChan)
}

func BenchmarkEnqueue2(b *testing.B) {
	wp := New(2)
	defer wp.Stop()
	releaseChan := make(chan struct{})

	b.ResetTimer()

	// Start workers, and have them all wait on a channel before completing.
	for i := 0; i < b.N; i++ {
		for i := 0; i < 64; i++ {
			wp.Submit(func() { <-releaseChan })
		}
		for i := 0; i < 64; i++ {
			releaseChan <- struct{}{}
		}
	}
	close(releaseChan)
}

func BenchmarkExecute1Worker(b *testing.B) {
	wp := New(1)
	defer wp.Stop()
	var allDone sync.WaitGroup
	allDone.Add(b.N)

	b.ResetTimer()

	// Start workers, and have them all wait on a channel before completing.
	for i := 0; i < b.N; i++ {
		wp.Submit(func() {
			time.Sleep(time.Millisecond)
			allDone.Done()
		})
	}
	allDone.Wait()
}

func BenchmarkExecute2Worker(b *testing.B) {
	wp := New(2)
	defer wp.Stop()
	var allDone sync.WaitGroup
	allDone.Add(b.N)

	b.ResetTimer()

	// Start workers, and have them all wait on a channel before completing.
	for i := 0; i < b.N; i++ {
		wp.Submit(func() {
			time.Sleep(time.Millisecond)
			allDone.Done()
		})
	}
	allDone.Wait()
}

func BenchmarkExecute4Workers(b *testing.B) {
	wp := New(4)
	defer wp.Stop()
	var allDone sync.WaitGroup
	allDone.Add(b.N)

	b.ResetTimer()

	// Start workers, and have them all wait on a channel before completing.
	for i := 0; i < b.N; i++ {
		wp.Submit(func() {
			time.Sleep(time.Millisecond)
			allDone.Done()
		})
	}
	allDone.Wait()
}

func BenchmarkExecute16Workers(b *testing.B) {
	wp := New(16)
	defer wp.Stop()
	var allDone sync.WaitGroup
	allDone.Add(b.N)

	b.ResetTimer()

	// Start workers, and have them all wait on a channel before completing.
	for i := 0; i < b.N; i++ {
		wp.Submit(func() {
			time.Sleep(time.Millisecond)
			allDone.Done()
		})
	}
	allDone.Wait()
}

func BenchmarkExecute64Workers(b *testing.B) {
	wp := New(64)
	defer wp.Stop()
	var allDone sync.WaitGroup
	allDone.Add(b.N)

	b.ResetTimer()

	// Start workers, and have them all wait on a channel before completing.
	for i := 0; i < b.N; i++ {
		wp.Submit(func() {
			time.Sleep(time.Millisecond)
			allDone.Done()
		})
	}
	allDone.Wait()
}
