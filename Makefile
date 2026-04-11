.PHONY: proto test bench jepsen-build jepsen-test jepsen-help jepsen-run

JEPSEN_DIR := jepsen
JEPSEN_BINARY := ./clavis
JEPSEN_NODES ?= 127.0.0.1,127.0.0.1,127.0.0.1
JEPSEN_USER ?= ubuntu
JEPSEN_SSH_KEY ?= $(HOME)/.ssh/clavis_jepsen_key
JEPSEN_TIME_LIMIT ?= 120

proto:
	mkdir -p api/v1
	protoc -I api/proto \
    	   --go_out=. --go_opt=module=github.com/Mfon-19/clavis \
    	   --go-grpc_out=. --go-grpc_opt=module=github.com/Mfon-19/clavis \
    	   api/proto/lock.proto

	protoc --go_out=. --go_opt=paths=source_relative \
		   internal/raftlog/command.proto

test:
	go test ./...

jepsen-build:
	GOOS=linux GOARCH=arm64 go build -o $(JEPSEN_BINARY) ./cmd/clavis

jepsen-test:
	cd $(JEPSEN_DIR) && lein test

jepsen-help:
	cd $(JEPSEN_DIR) && lein run -- test --help

jepsen-run:
	cd $(JEPSEN_DIR) && lein run -- test \
	  --nodes $(JEPSEN_NODES) \
	  --username $(JEPSEN_USER) \
	  --ssh-private-key $(JEPSEN_SSH_KEY) \
	  --binary ../clavis \
	  --time-limit $(JEPSEN_TIME_LIMIT)
