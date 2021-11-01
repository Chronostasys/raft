package raft

type RPCEnd interface {
	Call(svcMeth string, args interface{}, reply interface{}) bool
}
