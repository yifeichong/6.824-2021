.SILENT:

.PHONY: test
test-mr:
	cd ./main && bash test-mr.sh

.PHONY: mrcoordinator
mrcoordinator:
	cd ./main && \
	go build -race -buildmode=plugin ../mrapps/$(app_source) && \
	rm -f rm-out* && \
	echo "*** Starting mr coordinator" && \
	go run -race mrcoordinator.go pg-*.txt

.PHONY: mrworker
mrworker:
	echo "--- Starting mr worker" && \
	go run -race mrworker.go ./$(compiled)
