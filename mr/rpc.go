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

// Add your RPC definitions here.
// Args holds the list of intermediate <-- TODO: add list of intermediate filenames to the args
type Task struct {
	Index      string
	FileName   string
	StringRepr string
	Mode       int // NOTE: enum for operation type - Map or Reduce
}

func NewTask(filename, index string, mode int) Task {
	return Task{
		Index:      index,
		FileName:   filename,
		StringRepr: index + filename,
		Mode:       mode,
	}
}

type Args struct {
	Task Task
}

type Reply struct {
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
