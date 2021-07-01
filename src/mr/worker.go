package mr

import (
	"fmt"
	"hash/fnv"
	"net/rpc"

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// init goroutines pools and select to

	// make Rpc Call to coordinator and Get the Task
	go work(mapf, reducef)

}

// work is real process function in order to talk with coordinator
// Infinite loop to keep worker running unless ask the task from coordinator failed for 3 times
func work(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		// ask task from master, try to get a test. If failed, try some times and if all failed, exit.
		taskRequest, taskResponse := &AskTaskRequest{}, &AskTaskResponse{}

		ok := rpcCaller(AssignWorks, taskRequest, taskResponse)
		for times := 0; !ok && times < RpcRetryTimes; times++ {
			log.Error("Rpc Caller Error: AssignWorks Error, Start Retry")
			ok = rpcCaller(AssignWorks, taskRequest, taskResponse)
		}

		// retry 3 times and still fails
		if !ok {
			return
		}

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

// mapWorker is
func mapWorker(mapf func(string, string) []KeyValue, params *AskTaskResponse) error {

	// TODO:

	return nil
}

func reduceWorker(reducef func(string, []string) string, params *AskTaskResponse) error {

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
