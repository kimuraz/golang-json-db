install:
	go build -o gjdb
	mv gjdb /usr/local/bin

uninstall:
	rm /usr/local/bin/gjdb

run-server:
	go run . server start

run-client:
	go run . client start

cli-help:
	go run . help

help:
	@echo "make install - install the binary"
	@echo "make uninstall - uninstall the binary"
	@echo "make run-server - run the server"
	@echo "make run-client - run the client"
	@echo "make run cli-help - run the cli with help message"
	@echo "make help - display this help message"