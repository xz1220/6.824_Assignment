package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (

	/*
	 Status translation:
	 Task: 0, 1, 2
	 Worker: 0, 1, 2, 3

	 For Task : 0 -> 1 -> 2 -> 0
	 For Worker : 0 -> 1 -> 2 -> 0 & if somethings bad happend, 0、1、2 -> 3

	 Woker will init a new gorountine and notify the coordinator during the heartbeat machnism. The worker processor will create a new
	 worker to ask a new task. Task status will be reset to 0 and waitting to be assigned.
	*/

	Idle      = 0 // nothing to do
	InProcess = 1 // Process a task
	Completed = 2 // has completed
	Error     = 3 // only happend in the goroutines

	/*
	 TaskType
	*/
	MapTask    = 1
	ReduceTask = 2

	RpcRetryTimes = 3
)

type Coordinator struct {
	MapTasks         map[int64]string // MapTaskID and filePath
	ReduceTasks      map[int64]string // ReduceTask
	MapTaskStatus    map[int64]int64  // Task status
	ReduceTaskStatus map[int64]int64  //
	WorkerStatus     map[int64]int64  // worker status
	IdleWorker       []int64          // idle worker queue
	TotalWorkers     int64

	AllMapOk    bool //
	AllReduceOk bool //
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	for {
		if c.AllMapOk && c.AllReduceOk {
			return true
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}

// Your code here -- RPC handlers for the worker to call.

// AssigneWorks traverses all maptasks first and then all reduce tasks, try to find a idle task to assigned
// if found, change the worker status and map staus
func (c *Coordinator) AssignWorks(args *AskTaskRequest, reply *AskTaskResponse) error {

	// if worker is new, assign worker ID and init status
	if args.WorkerID == 0 {
		if len(c.IdleWorker) == 0 {
			log.Fatalf("Init Error: no new workerIds, init worker failed")
			return errors.New("no new workers, init failed")
		}

		reply.WorkerID = c.IdleWorker[0]
		// if everything ok, change the staus when return
		defer func() {
			c.IdleWorker = c.IdleWorker[1:]
			c.WorkerStatus[reply.WorkerID] = InProcess
		}()
	}

	taskID, taskType := c.FindTheTask()
	if taskID == -1 {
		log.Fatal("")
	}

	return nil
}

func (c *Coordinator) FindTheTask() (int64, int64) {
	if taskID := c.FindMapTasks(); taskID != -1 {
		return taskID, MapTask
	}

	if taskID := c.FindReduceTasks(); taskID != -1 {
		return taskID, ReduceTask
	}

	return -1, -1
}

/*

*/
func (c *Coordinator) IsFinished() bool {
	if c.FindMapTasks() == -1 && c.FindReduceTasks() == -1 {
		return true
	}

	return false
}

func (c *Coordinator) FindMapTasks() int64 {
	for key, val := range c.MapTaskStatus {
		if val == Idle {
			return key
		}
	}
	
	// update the MapStatus
	c.AllMapOk = true
	return -1
}

func (c *Coordinator) FindReduceTasks() int64 {
	for key, val := range c.ReduceTaskStatus {
		if val == Idle {
			return key
		}
	}

	// update the reduceStatus
	c.AllReduceOk = true
	return -1
}
