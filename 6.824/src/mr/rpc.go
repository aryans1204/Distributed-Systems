package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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


type GetReduceCountArgs struct {

}

type GetReduceCountReply struct {
	ReduceCount int
}

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	TaskType TaskType
	TaskId int
	TaskFile string
}

type ReportTaskArgs struct {
	WorkerId int
	TaskType TaskType
	TaskId int
}

type ReportTaskReply struct {
	CanExit bool
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
