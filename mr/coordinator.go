package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type concurrentMap struct {
	mx    sync.RWMutex
	items map[string]bool
}

func newConcurrentMap() *concurrentMap {
	return &concurrentMap{
		items: make(map[string]bool),
	}
}

func (m *concurrentMap) len() int {
	m.mx.RLock()
	defer m.mx.RUnlock()
	return len(m.items)
}

func (m *concurrentMap) get(key string) bool {
	m.mx.RLock()
	defer m.mx.RUnlock()
	val, has := m.items[key]
	if !has {
		return false
	}
	return val
}

func (m *concurrentMap) set(key string) {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.items[key] = true
}

func (m *concurrentMap) pop(key string) {
	m.mx.Lock()
	defer m.mx.Unlock()
	delete(m.items, key)
}

type coordinatorConstants struct {
	mx      sync.RWMutex
	nMap    int
	nReduce int
	nTotal  int
	timeout int
}

func (c *coordinatorConstants) getTotalTasksAmount() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.nTotal
}

type Coordinator struct {
	constants       *coordinatorConstants
	mapTasksToDo    chan string
	reduceTasksToDo chan string
	tasksInProgress *concurrentMap
	tasksDone       *concurrentMap
}

func (c *Coordinator) restartTaskByTimeout(args Args) {
	c.constants.mx.RLock()
	timeout := c.constants.timeout
	c.constants.mx.RUnlock()
	elapsedTimeSeconds := 0
	for {
		if elapsedTimeSeconds >= timeout && !c.tasksDone.get(args.FileNamePattern) {
			switch args.Mode {
			case Map:
				c.mapTasksToDo <- args.FileNamePattern
			case Reduce:
				c.reduceTasksToDo <- args.FileNamePattern
			}
			c.tasksInProgress.pop(args.FileNamePattern)
			return
		}
		time.Sleep(time.Second * 1)
		elapsedTimeSeconds++
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ScheduleTask(args *Args, reply *Reply) error {
	if !args.Done && c.tasksInProgress.len() > 0 {
		switch args.Mode {
		case Map:
			c.mapTasksToDo <- args.FileNamePattern
		case Reduce:
			c.reduceTasksToDo <- args.FileNamePattern
		}
		c.tasksInProgress.pop(args.FileNamePattern)
		return nil
	}
	c.tasksInProgress.pop(args.FileNamePattern)
	c.tasksDone.set(args.FileNamePattern)
	select {
	case fname := <-c.mapTasksToDo:
		reply.Mode = Map
		reply.FileNamePattern = fname
		c.constants.mx.RLock()
		reply.NReduce = c.constants.nReduce
		c.constants.mx.RUnlock()
		c.tasksInProgress.set(fname)
	case fname := <-c.reduceTasksToDo:
		reply.Mode = Reduce
		reply.FileNamePattern = fname
		c.tasksInProgress.set(fname)
	default:
		return nil
	}
	c.restartTaskByTimeout(*args)
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
	ret := false
	if c.constants.getTotalTasksAmount() == c.tasksDone.len() {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		constants: &coordinatorConstants{
			nMap:    len(files),
			nReduce: nReduce,
			nTotal:  nReduce * len(files),
			timeout: 10, // NOTE: wait 10 seconds max, then re-schedule task
		},
		mapTasksToDo:    make(chan string, len(files)),
		reduceTasksToDo: make(chan string, nReduce),
		tasksInProgress: newConcurrentMap(),
		tasksDone:       newConcurrentMap(),
	}

	for _, file := range files {
		c.mapTasksToDo <- file
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasksToDo <- string(i)
	}

	c.server()
	return &c
}
