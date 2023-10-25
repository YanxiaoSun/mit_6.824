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

// Add your RPC definitions here.

type WorkerArgs struct{

}
type WorkerReply struct{
	TaskType int // 0:Map 1:Reduce 2:Wait 3:Finished
	Filename string // Map task
	Nmap int    // number of map task
	Nreduce int // number of reduce task
	MapTaskId int 
	ReduceTaskId int

}

type FinishedArgs struct{
	IsMap bool
	Id int
}
type FinishedReply struct{
	
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
