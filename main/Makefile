.SILENT:

.PHONY: mrcoordinator
mrcoordinator:
	rm -f mr-out* && \
	echo "*** Starting mr coordinator" && \
	go run -race mrcoordinator.go pg-*.txt

.PHONY: mrworker
mrworker:
	echo "--- Starting mr worker" && \
	go run -race mrworker.go $(compiled)
