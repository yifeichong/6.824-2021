package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type concurrentMap struct {
	mx    sync.RWMutex
	items map[string]int
}

func newConcurrentMap() *concurrentMap {
	return &concurrentMap{
		items: make(map[string]int),
	}
}

func (m *concurrentMap) len() int {
	m.mx.RLock()
	defer m.mx.RUnlock()
	return len(m.items)
}

func (m *concurrentMap) get(key string) int {
	m.mx.RLock()
	defer m.mx.RUnlock()
	val, has := m.items[key]
	if !has {
		return -1
	}
	return val
}

func (m *concurrentMap) set(key string, val int) {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.items[key] = val
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

func (c *coordinatorConstants) getMapTasksAmount() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.nMap
}

type Coordinator struct {
	constants       *coordinatorConstants
	mapTasksToDo    chan Task
	reduceTasksToDo chan Task
	tasksInProgress *concurrentMap
	tasksDone       *concurrentMap
}

func (c *Coordinator) restartTaskByTimeout(reply Reply) {
	c.constants.mx.RLock()
	timeout := c.constants.timeout
	c.constants.mx.RUnlock()
	elapsedTimeSeconds := 0
	for {
		val := c.tasksDone.get(reply.Task.FileName)
		if elapsedTimeSeconds >= timeout && val == -1 {
			switch reply.Mode {
			case Map:
				c.mapTasksToDo <- reply.Task
			case Reduce:
				c.reduceTasksToDo <- reply.Task
			default:
				return
			}
			c.tasksInProgress.pop(reply.Task.FileName)
			log.Printf("(%v) Task with filename `%v` has been canceled", reply.Mode, reply.Task.FileName)
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
	reply.Mode = None
	reply.Task = Task{}
	if len(c.mapTasksToDo) > 0 {
		reply.Task = <-c.mapTasksToDo
		reply.Mode = Map
		c.constants.mx.RLock()
		reply.NReduce = c.constants.nReduce
		c.constants.mx.RUnlock()
		c.tasksInProgress.set(reply.Task.FileName, reply.Task.Index)
		go c.restartTaskByTimeout(*reply)

		// TODO: remove
		log.Println("MAP", len(c.mapTasksToDo), len(c.reduceTasksToDo), c.tasksInProgress.len(), c.tasksDone.len())

		return nil
	}
	if (len(c.reduceTasksToDo) > 0) && (c.tasksDone.len() >= c.constants.getMapTasksAmount()) {
		reply.Task = <-c.reduceTasksToDo
		reply.Mode = Reduce
		c.tasksInProgress.set(reply.Task.FileName, reply.Task.Index)
		go c.restartTaskByTimeout(*reply)

		// TODO: remove
		log.Println("REDUCE", len(c.mapTasksToDo), len(c.reduceTasksToDo), c.tasksInProgress.len(), c.tasksDone.len())
	}
	return nil
}

func (c *Coordinator) CommitTask(args *Args, reply *Reply) error {
	if args.Mode != None {
		c.tasksInProgress.pop(args.Task.FileName)
		c.tasksDone.set(args.Task.FileName, args.Task.Index)
	}
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
		mapTasksToDo:    make(chan Task, len(files)),
		reduceTasksToDo: make(chan Task, nReduce),
		tasksInProgress: newConcurrentMap(),
		tasksDone:       newConcurrentMap(),
	}

	for i, file := range files {
		c.mapTasksToDo <- Task{FileName: file, Index: i}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasksToDo <- Task{FileName: strconv.Itoa(i), Index: i}
	}

	c.server()
	return &c
}
