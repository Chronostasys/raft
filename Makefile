.PHONY: kvraft kvraftcli
all: kvraft kvraftcli

kvraft: cmd/kvraft/kvraft.go
	go build -o out/kvraft/kvraft cmd/kvraft/kvraft.go

kvraftcli: cmd/kvraft/cli/cli.go
	go build -o out/kvraft/cli cmd/kvraft/cli/cli.go

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
	go test -bench=. -run=RaftStart

benchmark-kv:
	cd kvraft/ && time go test -bench=. -run=Append -benchtime=10000x