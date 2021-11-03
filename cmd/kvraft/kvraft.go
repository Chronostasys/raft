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
		println("A highly available kv server based on raft")
		println("To use it, you need to start at least 3 kvraft instance")
		println("The number of kvraft must be odd number")
		println("Usage:")
		println("	$kvraft [me] [endpoint1] [endpoint2] [endpoint3]...")
		println("Example:")
		println("	$kvraft 0 :1234 :1235 :1236")
		println("	$kvraft 1 :1234 :1235 :1236")
		println("	$kvraft 2 :1234 :1235 :1236")
		os.Exit(1)
	}
	ends := os.Args[2:]
	rpcends := raft.MakeRPCEnds(ends)
	me, _ := strconv.Atoi(os.Args[1])
	kv := kvraft.StartKVServer(rpcends, me, raft.MakrRealPersister(me), 1000)
	kv.Serve(ends[me])
}
