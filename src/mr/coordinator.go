package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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

	Idle      int64 = 0 // nothing to do
	InProcess int64 = 1 // Process a task
	Completed int64 = 2 // has completed
	Error     int64 = 3 // only happend in the goroutines

	/*
	 TaskType For worker to choose run mapFunc or reduceFunc
	*/
	MapTask    int64 = 1
	ReduceTask int64 = 2

	MaxRetryTimes int64 = 3

	/*
	 Coordinator RPC Function
	*/
	AssignWorks string = "Coordinator.AssignWorks"

	/*
	 some useful defination
	*/
	EmptyPath     string = ""
	TaskNotFound  int64  = -1
	FatalTaskType int64  = -1
	FileWriterKey string = "syncWriter"
)

var (
	NReduceTask int64 // init when mrcoordinator call MkCoordinator and pass nReduce
	NMapTask    int64 // Map

	CurrentTaskID   int64        // TaskID for assign
	CurrentWorkerID int64        //
	mutex           sync.RWMutex //
)

type TaskInfo struct {
	TaskID       int64
	TaskFilePath string
	TaskStatus   int64
}

type Coordinator struct {
	MapTasks sync.Map // MapTaskID -> Task
	AllMapOk bool     //

	ReduceTasks sync.Map // ReduceTask
	AllReduceOk bool     //

	WorkerStatus sync.Map // workID -> work status
	IdleWorker   []int64  // idle worker queue
	TotalWorkers int64    // total worker number

	WorkerMapFiles sync.Map
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
	if c.AllMapOk && c.AllReduceOk {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// init some status

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

		// get the first ID from idle worker queue
		reply.WorkerID = c.IdleWorker[0]
		// if everything ok, change the staus when return
		defer func() {
			c.IdleWorker = c.IdleWorker[1:]
			c.WorkerStatus.Store(reply.WorkerID, InProcess)
		}()
	}

	// TODO: try to assign work and update the status
	taskID, taskType := c.FindTheTask()
	if taskID == TaskNotFound || taskType == FatalTaskType {
		log.Fatal("All Task Has Been Done")
		reply.TaskPath = EmptyPath
		reply.TaskType = FatalTaskType
		return nil
	}

	reply.TaskType = taskType
	if taskType == MapTask {
		taskPath, ok := c.MapTasks.Load(taskID)
		if !ok {
			log.Printf("System Error: Load key error - %v", taskID)
		}

		reply.TaskPath, ok = taskPath.(string)
		if !ok {
			log.Printf("System Error: type assertion error - %v", taskPath)
		}

	} else if taskType == ReduceTask {
		taskPath, ok := c.ReduceTasks.Load(taskID)
		if !ok {
			log.Printf("System Error: Load key error - %v", taskID)
		}

		reply.TaskPath, ok = taskPath.(string)
		if !ok {
			log.Printf("System Error: type assertion error - %v", taskPath)
		}
	}

	return nil
}

// TaskAck is a func that worker return the result by this func.
func (c *Coordinator) TaskAck(args *TaskAckRequest, reply *TaskAckResponse) error {
	if args.WorkerID == 0 {
		return errors.New("Params Error: WorkerID equals zero!")
	}

	if args.TaskType == MapTask {

	} else if args.TaskType == ReduceTask {

	} else {
		return errors.New("Params Error: Task Type Error")
	}

	return nil
}

//FindTheTask find the idle task in map tasks first and the find the idle task in reduce tasks.
func (c *Coordinator) FindTheTask() (int64, int64) {
	if taskID := c.FindIdleMapTasks(); taskID != TaskNotFound {
		return taskID, MapTask
	}

	if taskID := c.FindIdleReduceTasks(); taskID != -1 {

		return taskID, ReduceTask
	}

	return TaskNotFound, FatalTaskType
}

/*
 Heartbeat module
*/

// TODO：Implement Heartbeat module

/**
Task Management Module, the main function is to find, update the MapTask and ReduceTask.
1. Find module: find the idle map or reduce task, and if all tasks has been done, update the status . The IsFinished function will check allMapOk and AllReduceOk.
2. Update module:
*/

//IsFinished judge whether all the task has completed.
func (c *Coordinator) IsFinished() bool {
	if c.FindIdleMapTasks() == TaskNotFound && c.FindIdleReduceTasks() == TaskNotFound {
		return true
	}

	return false
}

//FindIdleMapTasks use findIdleTasks in the idle worker.
func (c *Coordinator) FindIdleMapTasks() int64 {

	idleTaskId := FindIdleTasks(&c.MapTasks)
	if idleTaskId == TaskNotFound {
		c.AllMapOk = true
	}

	return idleTaskId
}

//FindIdleReduceTasks use findIdleTasks in the idle worker.
func (c *Coordinator) FindIdleReduceTasks() int64 {

	idleTaskId := FindIdleTasks(&c.ReduceTasks)
	if idleTaskId == TaskNotFound {
		c.AllReduceOk = true
	}

	return idleTaskId
}

func (c *Coordinator) InitMapTasks(files []string) {
	for _, file := range files {
		c.MapTasks.Store(NewTaskID(), file)
	}
}

func (c *Coordinator) InitReduceTasks(nReduce int64) {
	for i:=0 ; i < nReduce ; i++ {

	}
}


/**
	Utils module: contains some helper function.
 */

//FindIdleTasks range the map and return the idle workerId
func FindIdleTasks(syncMap *sync.Map) int64 {

	// idle worker id
	var idelWorkerId int64

	// range the map and find the idle worker
	syncMap.Range(func(key, value interface{}) bool {
		switch value.(type) {
		case TaskInfo:
			value, ok := value.(TaskInfo)
			if ok && value.TaskStatus == Idle {
				return false
			}
		}
		return true
	})

	// worker id is not 0
	if idelWorkerId != 0 {
		return idelWorkerId
	}

	return TaskNotFound
}

//NewTaskID return a new taskID.
func NewTaskID() int64 {
	mutex.Lock()
	defer mutex.Unlock()
	CurrentTaskID++
	return CurrentTaskID
}

func NewWorkerID() int64 {
	mutex.Lock()
	defer mutex.Unlock()
	CurrentWorkerID ++
	return CurrentWorkerID
}