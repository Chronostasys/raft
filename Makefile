.PHONY: kvraft kvraftcli
all: kvraft kvraftcli

kvraft: cmd/kvraft/kvraft.go
	go build -o out/kvraft/kvraft cmd/kvraft/kvraft.go

kvraftcli: cmd/kvraft/cli/cli.go
	go build -o out/kvraft/cli cmd/kvraft/cli/cli.go

clean: 
	rm -rf out/