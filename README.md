# MIT 6.824 Distributed systems  

## 1. Map Reduce  

### How to run  
Implementation of map reduce is inside `./mr` directory. It consists of coordinator and worker.  
Test map reduce implementation:  
```
cd ./main 
bash test-mr.sh
```  
Implementation of the map reduce should produce the same output as a sequential one (`./main/mrsequential.go`).  
To run map-reduce, first compile the plugin with map and reduce functions (placed in `./mrapps`):  
```
go build -race -buildmode=plugin ../mrapps/wc.go
go build -race mrworker.go
go build -race mrcoordinator.go
```  
Then you're ready to run the coordinator (master) process:
```
rm -f mr-*
go run -race mrcoordinator.go pg-*.txt
```  
And then run several workers in the other shells passing file name of the pre-built app (placed in `./main` after being built):  
```
go run -race mrworker.go ./wc.so
```  

### How it works  
  - we spawn several workers and a single coordinator (a.k.a master) process;  
  - client program provides list of files and number of reducers to master;  
  - each worker in a loop makes task request to master;  
  - while initializing, master fills the tasks channels with the passed filenames and gives tasks to workers by request;  
  - master stores five "lists" of tasks: *map_todo*, *reduce_todo*, *map_failed*, *reduce_failed* and *done*. If for some reason task execution has been failed - master just adds the failed task to the *failed* channels;  
  - during execution, workers store intermidiate and final results locally, followed by certain naming pattern;  
  - after both map and reduce stages finished - coordinator terminates and workers termites too, when they can't reach the master;  

## 2. Raft  

Navigate to the `./raft` directory first.  
Run tests:  
```
go test -race
```  
You can also run tests separately by providing the test name prefix:  
```  
go test -run <TEST_NAME_PREFIX> -race
```  
Possible prefixes:  
 - `2A` - leader election;  
 - `2B` - log replication;  
 - `2C` - persistence;  
 - `2D` - log compaction;  
