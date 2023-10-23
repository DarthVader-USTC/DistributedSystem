package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	Idle     = 0
	Running  = 1
	Finished = 2
)

const (
	MapTask    = 0
	ReduceTask = 1
)

type TaskArgs struct {
	Status   int //idle, running, finished
	TaskType int //send when finished
	TaskId   int //send when finished
}

type TaskReply struct {
	TaskType int //send when idle
	TaskId   int //send when idle
	WorkerId int //send when idle for both Map and Reduce Task, used to get distinct intermidiate file name,but maybe it doesn't need the WorkId to give the temp file distinct name
	Map      int
	Reduce   int
	Filename string //send when idle for Map Task
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
