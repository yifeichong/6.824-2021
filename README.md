## MIT 6.824 Distributed systems  

### Map Reduce  
Implementation of map reduce is inside `./mr` directory. It consists of coordinator and worker.  
Test map reduce implementation:  
```
make test-mr
```  
Implementation of the map reduce should produce the same output as a sequential one (`./main/mrsequential.go`).  
To run map-reduce first run coordinator process (master) passing the source file name with map and reduce functions (placed in `./mrapps`):  
```
make mrcoordinator app_source=wc.go
```  
And then run several workers in the other shells passing file name of the pre-built app code (placed in `./main` after running coordinator):  
```
make mrworker compiled=wc.so
```  
