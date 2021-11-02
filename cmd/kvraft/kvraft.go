package main

import (
	"os"
	"strconv"

	"github.com/Chronostasys/raft"
	"github.com/Chronostasys/raft/kvraft"
)

func main() {
	if len(os.Args) < 3 {
		println("kvraft server")
		println("Usage:")
		println("	$kvraft [me] [endpoint1] [endpoint2] [endpoint3]...")
		println("Example:")
		println("	$kvraft 0 :1234 :1235 :1236")
		os.Exit(1)
	}
	ends := os.Args[2:]
	rpcends := raft.MakeRPCEnds(ends)
	me, _ := strconv.Atoi(os.Args[1])
	kv := kvraft.StartKVServer(rpcends, me, raft.MakePersister(), 1000)
	kv.Serve(ends[me])
}
