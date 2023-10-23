package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	Status    int       // idle, running, finished
	TaskType  int       //MapTask, ReduceTask
	TaskId    int       //TaskId
	FileName  string    //FileName for Map Task
	StartTime time.Time //start time,when running time is too long, assign Task status to idle for re-scheduling
}
type Coordinator struct {
	// Your definitions here.
	TaskList       []Task
	mutex          sync.Mutex
	nMap           int
	nReduce        int
	WorkerId       int
	MapFinished    int
	ReduceFinished int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FindIdleTask() (int, bool) {
	var TaskIndex int
	TaskIndex = -1
	fmt.Println("before lock")
	defer c.mutex.Unlock()
	c.mutex.Lock()
	fmt.Println("after lock")
	for i := range c.TaskList {
		if c.TaskList[i].Status == Idle {
			TaskIndex = i
			break
		}
	}
	if TaskIndex == -1 {
		return TaskIndex, false
	}
	return TaskIndex, true
}

func (c *Coordinator) InitCoordinator(files []string, nReduce int) {
	nMap := len(files)
	c.nMap = nMap
	c.nReduce = nReduce
	c.TaskList = make([]Task, nMap+nReduce)
	c.WorkerId = 0
	c.mutex = sync.Mutex{}
	for i := 0; i < nMap; i++ {
		c.TaskList[i].Status = Idle
		c.TaskList[i].TaskType = MapTask
		c.TaskList[i].TaskId = i
		c.TaskList[i].FileName = files[i]
	}
	for i := nMap; i < nMap+nReduce; i++ {
		c.TaskList[i].Status = Idle
		c.TaskList[i].TaskType = ReduceTask
		c.TaskList[i].TaskId = i - nMap
	}
}

func (c *Coordinator) TaskManager(args *TaskArgs, reply *TaskReply) error { //keep atomicity by mutex lock
	fmt.Println("TaskManager Start")
	fmt.Println(args.Status, args.TaskType, args.TaskId)
	defer c.mutex.Unlock()
	defer fmt.Println("TaskManager End")
	if args.Status == Idle {
		fmt.Println("Find Idle")
		i, ok := c.FindIdleTask()
		fmt.Println(i, ok)
		if !ok {
			reply.TaskType = -1
			reply.TaskId = i
			return nil
		}
		c.mutex.Lock()
		c.WorkerId++
		c.TaskList[i].Status = Running
		c.TaskList[i].StartTime = time.Now()
		reply.TaskType = c.TaskList[i].TaskType
		reply.TaskId = c.TaskList[i].TaskId
		reply.Map = c.nMap
		reply.Reduce = c.nReduce
		reply.WorkerId = c.WorkerId
		fmt.Println("WorkerId", reply.WorkerId)
		if c.TaskList[i].TaskType == MapTask {
			reply.Filename = c.TaskList[i].FileName
			fmt.Println(reply.Filename)
		}
		go func() { //if the task is running tool long, make the task state to idle again
			defer c.mutex.Unlock()
			time.Sleep(10 * time.Second)
			c.mutex.Lock()
			if c.TaskList[i].Status == Running {
				c.TaskList[i].Status = Idle
			}
		}()
	} else if args.Status == Finished {
		c.mutex.Lock()
		fmt.Println("Find Finished")
		if args.TaskType == MapTask {
			c.TaskList[args.TaskId].Status = Finished
			c.MapFinished++
		} else if args.TaskType == ReduceTask {
			c.TaskList[args.TaskId+c.nMap].Status = Finished
			c.ReduceFinished++
		}
	}
	return nil
}

func (c *Coordinator) TestRPC(args *ExampleArgs, reply *ExampleReply) error {
	fmt.Println("TestRPC")
	reply.Y = 100
	return nil

}

// start a thread that listens for RPCs from worker.go
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
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// TODO

	//fmt.Println("lock")
	defer c.mutex.Unlock()
	c.mutex.Lock()
	if c.ReduceFinished == c.nReduce {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := new(Coordinator)

	// Your code here.
	fmt.Println(files)
	c.InitCoordinator(files, nReduce)
	//c.Test()
	c.server()
	return c
}

func (c *Coordinator) Test() {
	fmt.Println(c.nMap, c.nMap)
	i, ok := c.FindIdleTask()
	fmt.Println(i, ok)
	fmt.Println(c.TaskList[i].Status)
}
