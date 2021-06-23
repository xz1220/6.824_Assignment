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

	// make Rpc Call to coordinator and Get the Task
	go work(mapf, reducef)
	// call mapf and store immediate result

	// call reducef and write to the answer files

}

// work is real process function in order to talk with coordinator
func work(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// ask task from master
	taskRequest, taskResponse := &AskTaskRequest{}, &AskTaskResponse{}

	if ok := rpcCaller("AssignWorks", taskRequest, taskResponse); !ok {
		log.Error("Rpc Caller Error: AssignWorks Error")
	}

	if taskResponse.TaskType == MapTask {

	} else if taskResponse.TaskType == ReduceTask {

	} else {
		// 消息类型错误
		log.Error("TaskType Error: Unknown Type!")
	}

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

/***************************  Example  ***************************************/

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		fmt.Println(err)
		return false
	}

	return true
}
