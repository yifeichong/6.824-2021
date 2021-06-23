package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readFile(fname string) (string, error) {
	file, err := os.Open(fname)
	defer file.Close()
	if err != nil {
		return "", err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func generateTmpFile() (string, error) {
	ofile, err := ioutil.TempFile("./", "mr.*")
	if err != nil {
		return "", err
	}
	defer ofile.Close()
	return ofile.Name(), nil
}

func writeIntermediateFile(fname string, kv KeyValue) error {
	ofile, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	defer ofile.Close()
	if err != nil {
		return err
	}
	enc := json.NewEncoder(ofile)
	err = enc.Encode(&kv)
	if err != nil {
		return err
	}
	return nil
}

func readIntermediateFile(fname string) ([]KeyValue, error) {
	file, err := os.Open(fname)
	defer file.Close()
	if err != nil {
		return nil, err
	}
	kva := []KeyValue{}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva, nil
}

func writeReduce(oname string, intermediate []KeyValue, reducef func(string, []string) string) error {
	ofile, err := ioutil.TempFile("./", "mr.*")
	if err != nil {
		return err
	}
	defer ofile.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		return err
	}
	return nil
}

func removeFiles(pattern string) error {
	fnames, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, fname := range fnames {
		os.Remove(fname)
	}
	return nil
}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func runMap(reply Reply, mapf func(string, string) []KeyValue) error {
	fnames, err := filepath.Glob(reply.Task.FileName)
	if err != nil {
		return err
	}
	if len(fnames) == 0 {
		return errors.New("")
	}
	for _, fname := range fnames {
		content, err := readFile(fname)
		if err != nil {
			return err
		}
		tempFnames := make(map[int]string)
		kva := mapf(fname, content)
		for _, kv := range kva {
			reduceTaskNumber := ihash(kv.Key) % reply.NReduce
			tempFname, ok := tempFnames[reduceTaskNumber]
			if !ok {
				tempFname, err = generateTmpFile()
				if err != nil {
					return err
				}
				tempFnames[reduceTaskNumber] = tempFname
			}
			err = writeIntermediateFile(tempFname, kv)
			if err != nil {
				return err
			}
		}
		for k, v := range tempFnames {
			intermediateFname := fmt.Sprintf("mr-%v-%v", reply.Task.Index, k)
			err = os.Rename(v, intermediateFname)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func runReduce(reply Reply, reducef func(string, []string) string) error {
	fileNamePattern := fmt.Sprintf("*-%v", reply.Task.Index)
	fnames, err := filepath.Glob(fileNamePattern)
	if err != nil {
		return err
	}
	if len(fnames) == 0 {
		return errors.New("")
	}
	intermediate := []KeyValue{}
	for _, fname := range fnames {
		kva, err := readIntermediateFile(fname)
		if err != nil {
			return err
		}
		intermediate = append(intermediate, kva...)
	}
	if len(intermediate) == 0 {
		return errors.New("")
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", reply.Task.Index)
	err = writeReduce(oname, intermediate, reducef)
	if err != nil {
		return err
	}
	// NOTE: after writing the final result, we can delete
	//       intermediate files for that reducer
	for _, fname := range fnames {
		os.Remove(fname)
	}
	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := Args{}
	reply := Reply{}
	success := call("Coordinator.GenerateWorkerID", &Args{}, &reply)
	if !success {
		return // NOTE: terminate if it's smth wrong with the coordinator
	}
	workerID := reply.WorkerID
	for {
		time.Sleep(time.Second * 1)
		success := call("Coordinator.ScheduleTask", &Args{WorkerID: workerID}, &reply)
		if !success {
			return // NOTE: terminate if it's smth wrong with the coordinator
		}
		switch reply.Mode {
		case Map:
			err := runMap(reply, mapf)
			if err != nil {
				continue
			}
		case Reduce:
			err := runReduce(reply, reducef)
			if err != nil {
				continue
			}
		default:
			continue
		}

		args = Args{
			Mode: reply.Mode,
			Task: reply.Task,
		}
		success = call("Coordinator.CommitTask", &args, &Reply{})
		if !success {
			return
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
