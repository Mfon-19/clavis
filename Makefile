.PHONY: proto test bench

proto:
	mkdir -p api/v1
	protoc -I api/proto \
    	   --go_out=. --go_opt=module=github.com/Mfon-19/clavis \
    	   --go-grpc_out=. --go-grpc_opt=module=github.com/Mfon-19/clavis \
    	   api/proto/lock.proto

	protoc --go_out=. --go_opt=paths=source_relative \
		   internal/raftlog/command.proto