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
	Index      string
	FileName   string
	StringRepr string
}

func NewTask(filename, index string) Task {
	return Task{
		Index:      index,
		FileName:   filename,
		StringRepr: index + filename,
	}
}

type Args struct {
	Mode     int // NOTE: enum for operation type - Map or Reduce
	WorkerID int64
	Task     Task
}

type Reply struct {
	Mode     int // NOTE: enum for operation type - Map or Reduce
	NReduce  int
	Task     Task
	WorkerID int64
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
