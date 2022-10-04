package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "os"
import "path/filepath"
import "sync"
import "time"

const TempDir = "tmp"
const TaskTimeout = 10

type TaskStatus int
type TaskType int
type JobStage int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

const (
	NotStarted TaskStatus = iota
	Executing
	Finished
)

type Task struct {
	Type TaskType
	Status TaskStatus
	WorkerId int
	Index int
	File string
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	mapTasks []Task
	reduceTasks []Task
	nMap int
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

// GetReduceCOunt RPC handler

func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *ReduceCountReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.ReduceCount = len(c.reduceTasks)

	return nil
}

// RequestTask RPC handler

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()

	var task *Task
	if c.nMap > 0 {
		task = c.selectTask(c.mapTasks, args.WorkerId)
	} else if c.nReduce > 0 {
			task = c.selectTask(c.reduceTasks, args.WorkerId)
	} else {
			task = &Task{ExitTask, Finished, -1, -1, ""}
	}

	reply.TaskType = task.Type
	reply.TaskFIle = task.File
	reply.taskId = task.Index

	c.mu.Unlock()
	go c.waitForTask(task)

	return nil
}

//ReportTaskDone RPC handler

func (c *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	if args.TaskType == MapTask {
		task = &c.mapTasks[args.TaskId]
	} else if args.TaskType == ReduceTask {
		task = &c.reduceTasks[args.TaskId]
	} else {
		fmt.Printf("Incorrect task type to report: %v\n", args.TaskType)
		return nil
	}

	if args.WorkerId == task.WorkerId && task.Status == Executing {
		task.Status = Finished
		if args.TaskType == MapTask && c.nMap > 0 {
			c.nMap--
		} else if args.TaskType == ReduceTask && c.nReduce > 0 {
				c.nReduce--
		}
	}
	reply.CanExit = c.nMap == 0 && c.nReduce == 0

	return nil
}

//selectTask

func (c *Coordinator) selectTask(taskList []Task, workerId int) *Task {
	var task *Task

	for i:=0; i < len(taskList); i++ {
		if taskList[i].Status == NotStarted {
			task = &taskList[i]
			task.Status = Executing
			task.WorkerId = workerId
			return task
		}
	}
	return &Task{NoTask, Finished, -1, -1, ""}
}
//waitForTask

func (c *Coordinator) waitForTask(task *Task) {
	if task.Type != MapTask && task.Type != ReduceTask {
		return
	}
	<-time.After(time.Second * TaskTimeout)

	c.mu.Lock()
	defer c.mu.Unlock()

	if task.Status == Executing {
		task.Status = NotStarted
		task.WorkerId = -1
	}
}
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
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.nMap == 0 && c.nReduce == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	nMap := len(files)
	c.nMap = nMap
	c.nReduce = nReduce
	c.mapTasks = make([]Task, 0, nMap)
	c.reduceTasks = make([]Task, 0, nReduce)

	for i:=0; i < nMap; i++ {
		mapTask := &Task{MapTask, NotStarted, -1, i, files[i]}
		c.mapTasks = append(c.mapTasks, mapTask)
	}
	for i:=0; i < nReduce; i++ {
		reducetask = &Task{Reducetask, NotStarted, -1, i, ""}
		c.reduceTasks = append(c.reduceTasks, reducetask)
	}

	c.server()

	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}
	return &c
}
