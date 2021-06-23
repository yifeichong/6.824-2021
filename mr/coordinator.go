package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type cMap struct {
	mx    sync.RWMutex
	items map[string]interface{}
}

func newConcurrentSet() *cMap {
	return &cMap{
		items: make(map[string]interface{}),
	}
}

func (m *cMap) len() int {
	m.mx.RLock()
	defer m.mx.RUnlock()
	return len(m.items)
}

func (m *cMap) set(key string, val interface{}) {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.items[key] = val
}

func (m *cMap) get(key string) (interface{}, bool) {
	m.mx.Lock()
	defer m.mx.Unlock()
	val, ok := m.items[key]
	return val, ok
}

func (m *cMap) pop(key string) {
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

func (c *coordinatorConstants) getNReduce() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.nReduce
}

func (c *coordinatorConstants) getTimeout() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.timeout
}

type Coordinator struct {
	constants       *coordinatorConstants
	mapTasksToDo    chan Task
	reduceTasksToDo chan Task
	tasksInProgress *cMap
	tasksDone       *cMap
	badTasks        *cMap
	lastWorkerID    int64
}

func (c *Coordinator) restartTaskByTimeout(reply Reply) {
	timeout := c.constants.getTimeout()
	elapsedTimeSeconds := 0
	for {
		_, has := c.tasksDone.get(reply.Task.StringRepr)
		if elapsedTimeSeconds >= timeout && !has {
			switch reply.Mode {
			case Map:
				// // TODO: remove
				// log.Println("MAP FAILED", reply.Task.FileName, reply.WorkerID)
				c.mapTasksToDo <- reply.Task

				// NOTE: place task in the black list
				_, hasTask := c.badTasks.get(reply.Task.StringRepr)
				if hasTask {
					c.tasksDone.set(reply.Task.StringRepr, nil)
				}
				c.badTasks.set(reply.Task.StringRepr, reply.WorkerID) // NOTE: keeps only last failed workerid

			case Reduce:
				// // TODO: remove
				// log.Println("REDUCE FAILED", reply.Task.Index, reply.WorkerID)
				c.reduceTasksToDo <- reply.Task
			default:
				return
			}

			c.tasksInProgress.pop(reply.Task.StringRepr)
			return
		}
		time.Sleep(time.Second * 1)
		elapsedTimeSeconds++
	}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GenerateWorkerID(args *Args, reply *Reply) error {
	id := atomic.LoadInt64(&c.lastWorkerID) + 1
	reply.WorkerID = id
	atomic.StoreInt64(&c.lastWorkerID, id)
	return nil
}

func (c *Coordinator) ScheduleTask(args *Args, reply *Reply) error {
	reply.Mode = None
	reply.WorkerID = args.WorkerID
	if len(c.mapTasksToDo) > 0 {
		task := <-c.mapTasksToDo
		reply.Task = task
		reply.Mode = Map
		reply.NReduce = c.constants.getNReduce()
		c.tasksInProgress.set(reply.Task.StringRepr, nil)
		go c.restartTaskByTimeout(*reply)
		return nil
	}
	if (len(c.reduceTasksToDo) > 0) && (c.tasksDone.len() >= c.constants.getMapTasksAmount()) {
		task := <-c.reduceTasksToDo
		reply.Task = task
		reply.Mode = Reduce
		c.tasksInProgress.set(reply.Task.StringRepr, nil)
		go c.restartTaskByTimeout(*reply)
		return nil
	}
	return nil
}

func (c *Coordinator) CommitTask(args *Args, reply *Reply) error {
	if args.Mode != None {
		c.tasksInProgress.pop(args.Task.StringRepr)
		c.tasksDone.set(args.Task.StringRepr, nil)
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
			nTotal:  nReduce + len(files),
			timeout: 10, // NOTE: wait 10 seconds max, then re-schedule task
		},
		mapTasksToDo:    make(chan Task, len(files)*2),
		reduceTasksToDo: make(chan Task, nReduce),
		tasksInProgress: newConcurrentSet(),
		tasksDone:       newConcurrentSet(),
		badTasks:        newConcurrentSet(),
	}

	for i, file := range files {
		c.mapTasksToDo <- NewTask(file, strconv.Itoa(i))
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasksToDo <- NewTask("@", strconv.Itoa(i))
	}

	c.server()
	return &c
}
