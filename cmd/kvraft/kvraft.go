package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"net/http"
	_ "net/http/pprof"

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
	kv := kvraft.StartKVServer(rpcends, me, raft.MakrRealPersister(me), 10000)
	go kv.Serve(ends[me])
	if len(os.Args) == 6 {
		go http.ListenAndServe(os.Args[5], nil)
	}
	println("start serving at", ends[me])
	println("persist data position:", fmt.Sprintf("%d.rast", me))
	println("ctrl+c to shutdown")
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
	<-s
	kv.Kill()
	fmt.Println("Shutting down gracefully.")

}
