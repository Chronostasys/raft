.PHONY: kvraft kvraftcli
all: kvraft kvraftcli

kvraft: cmd/kvraft/kvraft.go
	CGO_ENABLED=0 go build -o out/kvraft/kvraft cmd/kvraft/kvraft.go

kvraftcli: cmd/kvraft/cli/cli.go
	CGO_ENABLED=0 go build -o out/kvraft/cli cmd/kvraft/cli/cli.go

clean: 
	rm -rf out/

test-rf:
	time go test . -cover

test-rflab:
	time go test -run="2A|2B|2C" -cover

test-kvlab:
	cd kvraft/ && time go test -run="3A|3B" -cover

test-kv:
	cd kvraft/ && time go test . -cover

benchmark-rf:
	go test -bench=. -run=RaftStart -benchtime=3x

benchmark-kv:
	cd kvraft/ && time go test -bench=. -run=Benchmark -benchtime=100000x

pb: proto/*
	protoc --proto_path=proto proto/*.proto --go_out=plugins=grpc:.

ci-test:
	go test ./... -gcflags=all=-l -coverprofile=coverage.txt