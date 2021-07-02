package mr

import (
	"context"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"

	log "github.com/sirupsen/logrus"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

/*
 Go pool - The number of goroutine is controled by the NumWorker, which is larger than 1.
 Worker wiill init the pool and when a goroutine exit unexceptedly, a new goroutine will be created.

 TODO: This Go Pool rely on a infinite loop. So as for good use, it should provide a useful way to exit the pool.
		But may not used in this project.
*/

// Task contains params and function and should pass to GoPool
type Task struct {
	Worker func(context.Context)
	Params context.Context
}

func (t *Task) Run() {
	t.Worker(t.Params)
}

// GoPool is a routine pool, you should use like this :
// 1. pool := NewGoPool(nums)
// 2. go pool.Run()
// 3. pool.Put(Task)
type GoPool struct {
	MaxRoutine    int64
	Task          chan *Task
	ControlSignal chan int64
}

func (g *GoPool) Put(t *Task) {
	g.ControlSignal <- 1
	g.Task <- t
}

func (g *GoPool) Worker(t *Task) {
	t.Run()
	<-g.ControlSignal
}

func (g *GoPool) Run() {
	for {
		select {
		case t := <-g.Task:
			go g.Worker(t)
		}
	}
}

func NewGoPool(maxNum int64) *GoPool {
	Task := make(chan *Task, maxNum)
	ControlSignal := make(chan int64, maxNum)

	return &GoPool{
		MaxRoutine:    maxNum,
		Task:          Task,
		ControlSignal: ControlSignal,
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// init goroutines pools and start pool
	pool := NewGoPool(10)
	go pool.Run()

	// use infinite loop to try to create a  go routine
	ctx := context.Background()
	for {
		pool.Put(
			&Task{
				Params: context.WithValue(context.WithValue(ctx, MapTask, mapf), ReduceTask, reducef),
				Worker: work,
			},
		)
	}
}

// work is real process function in order to talk with coordinator
func work(ctx context.Context) {
	var workerID int64
	for {
		var mapf func(string, string) []KeyValue
		var reducef func(string, []string) string
		var ok bool

		if mapf, ok = ctx.Value(MapTask).(func(string, string) []KeyValue); !ok || mapf == nil {
			log.Error("Params Error: get map function from ctx error")
			return
		}

		if reducef, ok = ctx.Value(ReduceTask).(func(string, []string) string); !ok || reducef == nil {
			log.Error("Params Error: get reduce function from ctx error")
			return
		}

		// ask task from master, try to get a test. If failed, try some times and if all failed, exit.
		taskRequest, taskResponse := &AskTaskRequest{}, &AskTaskResponse{}

		if workerID != 0 {
			taskRequest.WorkerID = workerID
		}

		ok = rpcCaller(AssignWorks, taskRequest, taskResponse)
		for times := 0; !ok && times < RpcRetryTimes; times++ {
			log.Error("Rpc Caller Error: AssignWorks Error, Start Retry")
			ok = rpcCaller(AssignWorks, taskRequest, taskResponse)
		}

		// retry 3 times and still fails
		if !ok {
			return
		}

		// update workerID
		workerID = taskResponse.WorkerID

		// Do the work according to Type
		if taskResponse.TaskType == MapTask {
			err := mapWorker(mapf, taskResponse)
			if err != nil {
				log.Fatalf("Worker Error: MapWorker Error, WorkerID - %d - %v", taskResponse.WorkerID, err)
			}

		} else if taskResponse.TaskType == ReduceTask {
			err := reduceWorker(reducef, taskResponse)
			if err != nil {
				log.Fatalf("Worker Error: ReduceWorker Error, WorkerID - %d - %v", taskResponse.WorkerID, err)
			}

		} else {
			// 消息类型错误
			log.Error("TaskType Error: Unknown Type!")
		}
	}
}

func mapWorker(mapf func(string, string) []KeyValue, params *AskTaskResponse) error {
	
	// 读取文件
	file, err := os.Open(params.TaskPath)
	if err != nil {
		log.Fatalf("Cannot open %v", params.TaskPath)
		return fmt.Errorf("Cannot open %v", params.TaskPath)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", params.TaskPath)
	}
	
	// 调用Map函数
	kva := mapf(params.TaskPath, string(content))
	

	// 输出KV 应用ihash

	return nil
}

func reduceWorker(reducef func(string, []string) string, params *AskTaskResponse) error {

	// 

	return nil
}

// rpcCalller 参数调用封装
func rpcCaller(rpcFunc string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	// type assertion
	var ok bool
	switch args.(type) {
	case *AskTaskRequest:
		args, ok = args.(*AskTaskRequest)
		if !ok {
			log.Error("type assertion error")
		}

		reply, ok = reply.(*AskTaskResponse)
		if !ok {
			log.Error("type assertion error")
		}
	}

	err = c.Call(rpcFunc, args, reply)
	if err != nil {
		fmt.Println(err)
		return false
	}

	return true
}
