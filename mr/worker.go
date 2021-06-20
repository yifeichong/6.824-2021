package mr

import (
	"encoding/json"
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

func writeIntermediateFile(fname string, data []KeyValue) error {
	ofile, err := os.Create(fname)
	defer ofile.Close()
	if err != nil {
		return err
	}
	enc := json.NewEncoder(ofile)
	for _, kv := range data {
		err := enc.Encode(&kv)
		if err != nil {
			return err
		}
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

func reduceWriteOutput(fname string, intermediate []KeyValue, reducef func(string, []string) string) error {
	ofile, err := os.Create(fname)
	defer ofile.Close()
	if err != nil {
		return err
	}
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
	return nil

}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := Args{}
	reply := Reply{}
	for {
		time.Sleep(time.Second * 1)
		success := call("Coordinator.ScheduleTask", &Args{}, &reply)
		if !success {
			return // NOTE: terminate if it's smth wrong with the coordinator
		}

		// TODO: remove
		fmt.Println(reply)

		args = Args{
			Mode: reply.Mode,
			Task: reply.Task,
		}

		pwd, _ := os.Getwd()
		switch reply.Mode {
		case Map:
			fileNamePattern := filepath.Join(pwd, reply.Task.FileName)
			fnames, err := filepath.Glob(fileNamePattern)
			if err != nil || len(fnames) == 0 {
				continue
			}
			intermediate := make(map[int][]KeyValue)
			for _, fname := range fnames {
				content, err := readFile(fname)
				if err != nil {
					continue
				}
				kva := mapf(fname, content)
				for _, kv := range kva {
					reduceTaskNumber := ihash(kv.Key) % reply.NReduce
					intermediate[reduceTaskNumber] = append(intermediate[reduceTaskNumber], kv)
				}
			}
			// TODO: save in the format "mr-mapId-reduceId"
			for k, v := range intermediate {
				err = writeIntermediateFile(fmt.Sprintf("mr-%v", k), v)
				if err != nil {
					continue
				}
			}
		case Reduce:
			fileNamePattern := filepath.Join(
				pwd,
				fmt.Sprintf("*-%v", reply.Task.FileName),
			)
			fnames, err := filepath.Glob(fileNamePattern)

			// TODO: remove
			fmt.Println("REDUCE", reply.Mode, reply.Task, fnames)

			if err != nil || len(fnames) == 0 {
				continue
			}
			intermediate := []KeyValue{}
			for _, fname := range fnames {
				kva, _ := readIntermediateFile(fname)
				intermediate = append(intermediate, kva...)
			}
			if len(intermediate) == 0 {
				continue
			}
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%v", reply.Task.FileName)
			err = reduceWriteOutput(oname, intermediate, reducef)
			if err != nil {
				continue
			}
			// NOTE: after writing the final result, we can delete
			//       intermediate files for that reducer
			for _, fname := range fnames {
				os.Remove(fname)
			}
		}
		success = call("Coordinator.CommitTask", &args, &Reply{})
		if !success {
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
