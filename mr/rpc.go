package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const (
	Map = iota
	Reduce
	None
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

// Add your RPC definitions here.
// Args holds the list of intermediate
type Task struct {
	Index    int
	FileName string
}

type Args struct {
	Mode int // NOTE: enum for operation type - Map or Reduce
	Task Task
}

type Reply struct {
	Mode    int // NOTE: enum for operation type - Map or Reduce
	NReduce int
	Task    Task
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
