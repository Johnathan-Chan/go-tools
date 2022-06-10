package taskpool

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ITask 协程池任务接口
type ITask interface {
	Run(ctx context.Context, wait *sync.WaitGroup, errChan chan interface{})
}

type iTaskError struct {
	ITask
	Wait      *sync.WaitGroup
	ErrorChan chan interface{}
}

// TaskPool 协程池
type TaskPool struct {
	entryChan  chan iTaskError             // 协程池外部输入数据
	jobChan    chan iTaskError             // 分发任务给worker
	errMapChan map[string]chan interface{} // 管理异常的管道
	minPool    uint32                      // 协程池最小协程数
	maxPool    uint32                      // 协程池的最大协程数
	working    uint32                      // 正在忙的协程数量
	total      uint32                      // 当前协程池的协程数量
	timeout    time.Duration               // 扩展协程的超时时间
	increase   int                         // 每次扩展的个数
	once       sync.Once
	lock       sync.Mutex
	ptCtx      context.Context
}

// NewTaskPool 创建任务池
func NewTaskPool(min, max uint32, increase int, ptCtx context.Context, timeout time.Duration) *TaskPool {
	rand.Seed(time.Now().UnixNano())
	return &TaskPool{
		entryChan:  make(chan iTaskError, 1),
		jobChan:    make(chan iTaskError, 1),
		errMapChan: make(map[string]chan interface{}),
		minPool:    min,
		maxPool:    max,
		timeout:    timeout,
		increase:   increase,
		ptCtx:      ptCtx,
	}
}

// RunPool 启动协程池
// 保持最小容量, 无限扩容, 扩展的协程会在完成任务之后超时一段时间未有新任务之后释放
func (p *TaskPool) RunPool(ready ...func() error) error {
	for _, handler := range ready {
		if err := handler(); err != nil {
			log.Println("协程池启动失败")
			return err
		}
	}

	p.increaseWorker(int(p.minPool))

	go func() {
		for task := range p.entryChan {
			if p.working == p.total && p.total < p.maxPool {
				increase := p.increase
				diff := int(p.maxPool - p.total)
				if diff < p.increase {
					increase = diff
				}
				p.increaseWorker(increase)
			}
			p.jobChan <- task
		}
		p.StopPool()
	}()

	return nil
}

// StopPool 停止协程池
func (p *TaskPool) StopPool() {
	p.once.Do(func() {
		close(p.entryChan)
		close(p.jobChan)
	})
}

// 根据最小池启动指定个数的worker协程
func (p *TaskPool) increaseWorker(num int) {
	for i := 0; i < num; i++ {
		go func() {
			p.newWorker()
		}()
		atomic.AddUint32(&p.total, 1)
	}
}

// 新增worker协程
func (p *TaskPool) newWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	ret := time.Second * time.Duration(p.Rand(60))
	for {
		select {
		case task, quit := <-p.jobChan:
			if !quit {
				cancel()
				return
			}
			atomic.AddUint32(&p.working, 1)
			task.ITask.Run(ctx, task.Wait, task.ErrorChan)
			atomic.AddUint32(&p.working, ^uint32(int32(0)))
		case <-time.After(p.timeout + ret):
			// 随机超时释放空间
			if p.total > p.minPool {
				atomic.AddUint32(&p.total, ^uint32(int32(0)))
				cancel()
				return
			}
		case <-p.ptCtx.Done():
			cancel()
			return
		}
	}
}

// CreateErrorChannel 创建异常管道
func (p *TaskPool) CreateErrorChannel(name string) chan interface{} {
	p.lock.Lock()
	defer p.lock.Unlock()

	errChan, ok := p.errMapChan[name]
	if !ok {
		errChan = make(chan interface{}, 10)
		p.errMapChan[name] = errChan
	}
	return errChan
}

// GetErrorChannel 获取异常管道
func (p *TaskPool) GetErrorChannel(name string) chan interface{} {
	p.lock.Lock()
	defer p.lock.Unlock()
	if errChan, ok := p.errMapChan[name]; ok {
		return errChan
	}
	return nil
}

// Publish 执行任务
// @arg: task 任务
func (p *TaskPool) Publish(task ITask, wait *sync.WaitGroup, errChanName string) {
	p.entryChan <- iTaskError{
		ITask:     task,
		Wait:      wait,
		ErrorChan: p.CreateErrorChannel(errChanName),
	}
}

func (p *TaskPool) Rand(ranges int) int {
	return rand.Intn(ranges)
}

func (p *TaskPool) LogInfo() {
	log.Println("min: ", p.minPool, "total: ", p.total, "working: ", p.working)
}
