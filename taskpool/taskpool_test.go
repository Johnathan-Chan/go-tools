package taskpool

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"
)

type TaskBase struct {
	I int
	J int
}

// Run 任务
func (t *TaskBase) Run(context context.Context, wait *sync.WaitGroup, errChan chan interface{}){
	defer func() {
		wait.Done()
	}()
	errChan <- t
	time.Sleep(time.Second*2)
}

func TestTaskPool(t *testing.T) {
	taskpool := NewTaskPool(5, 10, 10, context.Background(), time.Second*20)
	taskpool.RunPool()

	go func() {
		pipline := taskpool.CreateErrorChannel("test")
		for message := range pipline{
			task := message.(*TaskBase)
			log.Println(task.I, task.J)
			taskpool.LogInfo()
		}
	}()

	wait := new(sync.WaitGroup)
	for i:=1; i<1000; i++{
		for j:=0; j<20; j++{
			task := &TaskBase{I: i, J:j}
			wait.Add(1)
			taskpool.Publish(task, wait, "test")
		}

		log.Println("等待")
		wait.Wait()
		log.Println("继续")
	}
}
