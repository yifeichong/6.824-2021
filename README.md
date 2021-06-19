# MIT 6.824 Distributed systems  

## Map Reduce  

### How to run  
Implementation of map reduce is inside `./mr` directory. It consists of coordinator and worker.  
Test map reduce implementation:  
```
make test-mr
```  
Implementation of the map reduce should produce the same output as a sequential one (`./main/mrsequential.go`).  
To run map-reduce first run coordinator process (master) passing the **source file** with map and reduce functions (placed in `./mrapps`):  
```
make mrcoordinator app_source=wc.go
```  
And then run several workers in the other shells passing file name of the pre-built app (placed in `./main` after running coordinator):  
```
make mrworker compiled=wc.so
```  

### How it works  
  - we spawn several workers and a single coordinator (a.k.a master) process;  
  - user provide list of files to master;  
  - each worker in a loop makes task request for master;  
  - while initializing, master fills the tasks channels with the passed filenames and gives tasks to workers by request;  
  - master stores three lists of tasks: *todo*, *inprogress* and *done*. If for some reason task execution has been failed - master just adds the failed task (filename pattern at our case) to the *todo* channel;  
  - during execution, workers stores intermidiate and final results locally, followed by certain naming pattern;  
  - after both map and reduce stages finished - coordinator terminates and workers termites too, when they can't reach the master;  
