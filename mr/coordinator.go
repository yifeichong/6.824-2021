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

type cSet struct {
	mx    sync.RWMutex
	items map[string]bool
}

func newCSet() *cSet {
	return &cSet{
		items: make(map[string]bool),
	}
}

func (m *cSet) len() int {
	m.mx.RLock()
	defer m.mx.RUnlock()
	return len(m.items)
}

func (m *cSet) set(key string) {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.items[key] = true
}

func (m *cSet) has(key string) bool {
	m.mx.Lock()
	defer m.mx.Unlock()
	val, ok := m.items[key]
	if !ok {
		return ok
	}
	return val && ok
}

func (m *cSet) pop(key string) {
	m.mx.Lock()
	defer m.mx.Unlock()
	delete(m.items, key)
}

type constants struct {
	mx      sync.RWMutex
	nMap    int
	nReduce int
	nTotal  int
	timeout int
}

func (c *constants) getTotalTasksAmount() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.nTotal
}

func (c *constants) getNMap() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.nMap
}

func (c *constants) getNReduce() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.nReduce
}

func (c *constants) getTimeout() int {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.timeout
}

type Coordinator struct {
	constants         *constants
	mapTasks          chan Task
	reduceTasks       chan Task
	failedMapTasks    chan Task
	failedReduceTasks chan Task
	tasksDone         *cSet
}

func (c *Coordinator) restartTaskByTimeout(task Task) {
	if task.Mode == None {
		return
	}
	timeout := c.constants.getTimeout()
	elapsedTimeSeconds := 0
	for {
		has := c.tasksDone.has(task.StringRepr)
		if elapsedTimeSeconds >= timeout && !has {
			switch task.Mode {
			case Map:
				c.failedMapTasks <- task
			case Reduce:
				c.failedReduceTasks <- task
			}
			return
		}
		time.Sleep(time.Second * 1)
		elapsedTimeSeconds++
	}
}

func (c *Coordinator) getCurrentStage() int {
	if (len(c.failedMapTasks) + len(c.mapTasks)) > 0 {
		return Map
	}
	nReduceWait := len(c.failedReduceTasks) + len(c.reduceTasks)
	if (c.tasksDone.len() >= c.constants.getNMap()) && (nReduceWait > 0) {
		return Reduce
	}
	return None
}

// NOTE: non-blocking value reading
func getTaskFromChan(ch chan Task) (Task, bool) {
	task := Task{}
	select {
	case task, ok := <-ch:
		if ok {
			return task, true
		}
		return task, false
	default:
		return task, false
	}
}

func selectFirstTask(chans []chan Task) (Task, bool) {
	task := Task{}
	for _, ch := range chans {
		task, ok := getTaskFromChan(ch)
		if ok {
			return task, ok
		}

	}
	return task, false
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) ScheduleTask(args *Args, reply *Reply) error {
	switch c.getCurrentStage() {
	case Map:
		task, ok := selectFirstTask([]chan Task{c.failedMapTasks, c.mapTasks})
		if ok {
			reply.Task = task
		}
		reply.NReduce = c.constants.getNReduce()
	case Reduce:
		task, ok := selectFirstTask([]chan Task{c.failedReduceTasks, c.reduceTasks})
		if ok {
			reply.Task = task
		}
	}
	go c.restartTaskByTimeout(reply.Task)
	return nil
}

func (c *Coordinator) CommitTask(args *Args, reply *Reply) error {
	if args.Task.Mode != None {
		c.tasksDone.set(args.Task.StringRepr)
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
	return c.constants.getTotalTasksAmount() == c.tasksDone.len()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		constants: &constants{
			nMap:    len(files),
			nReduce: nReduce,
			nTotal:  nReduce + len(files),
			timeout: 10, // NOTE: wait 10 seconds max, then re-schedule task
		},
		mapTasks:          make(chan Task, len(files)),
		reduceTasks:       make(chan Task, nReduce),
		failedMapTasks:    make(chan Task, len(files)),
		failedReduceTasks: make(chan Task, nReduce),
		tasksDone:         newCSet(),
	}
	for i, file := range files {
		c.mapTasks <- NewTask(file, strconv.Itoa(i), Map)
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks <- NewTask("@", strconv.Itoa(i), Reduce)
	}
	c.server()
	return &c
}
