package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"net/http"
	_ "net/http/pprof"

	"github.com/Chronostasys/raft"
	"github.com/Chronostasys/raft/kvraft"
)

func main() {
	readEnv()
	eps := flag.String("eps", ":1234 :1235 :1236", "raft peer end points")
	me := flag.Int("me", -1, "zero based index of endpoint")
	dbg := flag.String("pprof", "", "pprof port")
	reqlog := flag.Bool("log", false, "enable request log")
	flag.Parse()
	if *me == -1 {
		println("Usage of kvraft:")
		flag.PrintDefaults()
		return
	}
	ends := strings.Split(*eps, " ")
	rpcends := raft.MakeRPCEnds(ends)
	kv := kvraft.StartKVServer(rpcends, *me, raft.MakrRealPersister(*me, false), 400000)
	kv.EnableLog()
	ss := strings.Split(ends[*me], ":")
	if *reqlog {
		go kv.ServeWithReqLog(":" + ss[len(ss)-1])
	} else {
		go kv.Serve(":" + ss[len(ss)-1])
	}
	println("pid", os.Getpid())
	if len(*dbg) != 0 {
		go http.ListenAndServe(*dbg, nil)
	}
	println("start serving at", ends[*me])
	println("persist data position:", fmt.Sprintf("%d.rast", *me))
	println("ctrl+c to shutdown")
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
	<-s
	kv.Kill()
	fmt.Println("Shutting down gracefully.")

}

func readEnv() {
	me := os.Getenv("ME") + ".kv-hs"
	eps := os.Getenv("EPS")
	args := []string{}
	if len(me) > 0 && len(eps) > 0 {
		epl := strings.Split(eps, ";")
		for i, v := range epl {
			if strings.Contains(v, me) {
				args = append(args, "-me="+strconv.Itoa(i))
			}
		}
		args = append(args, "-eps="+strings.Join(epl, " "))
	}
	os.Args = append(os.Args, args...)
}
