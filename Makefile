.SILENT:

.PHONY: test
test-mr:
	cd ./main && bash test-mr.sh

.PHONY: mrcoordinator
mrcoordinator:
	go build -race -buildmode=plugin ./mrapps/wc.go
	rm ./main/rm-out*
	echo "*** Starting mr coordinator"
	go run -race ./main/mrcoordinator.go ./main/pg-*.txt

.PHONY: mrworker
mrworker:
	echo "--- Starting mr worker"
	go run -race ./main/mrworkr.go ./main/wc.so

.PHONY: run-mr
run-mr: mrcoordinator mrworker mrworker mrworker
