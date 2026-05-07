package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // index of last successful leader; tried first next time
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	return &Clerk{clnt: clnt, servers: servers}
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get cycles through the servers until one of them responds with a
// non-ErrWrongLeader reply. It keeps trying forever.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := rpc.GetReply{}
			ok := ck.clnt.Call(ck.servers[srv], "KVServer.Get", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			ck.leader = srv
			return reply.Value, reply.Version, reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Put rotates through servers; downgrades a retried ErrVersion to
// ErrMaybe per the lab spec — once we've sent more than one request,
// an ErrVersion reply could be due to our own earlier (lost-reply)
// success.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	retried := false
	for {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := rpc.PutReply{}
			ok := ck.clnt.Call(ck.servers[srv], "KVServer.Put", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				retried = true
				continue
			}
			ck.leader = srv
			if reply.Err == rpc.ErrVersion && retried {
				return rpc.ErrMaybe
			}
			return reply.Err
		}
		retried = true
		time.Sleep(100 * time.Millisecond)
	}
}
