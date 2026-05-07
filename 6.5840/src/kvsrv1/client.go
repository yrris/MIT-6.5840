package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	return &Clerk{clnt: clnt, server: server}
}

// Get keeps re-trying until it gets a reply from the server, then
// returns whatever the server replied with. Server errors (ErrNoKey)
// surface to the caller verbatim.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for {
		reply := rpc.GetReply{}
		if ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply) {
			return reply.Value, reply.Version, reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Put keeps re-trying until a reply arrives. If the very first reply is
// ErrVersion the Put never executed and we surface ErrVersion. If we
// already had to re-send (so an earlier attempt may have been processed
// but its reply lost) and the server now returns ErrVersion, we cannot
// tell whether our own write or someone else's is responsible, so we
// downgrade to ErrMaybe per the lab spec.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	retried := false
	for {
		reply := rpc.PutReply{}
		if ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply) {
			if reply.Err == rpc.ErrVersion && retried {
				return rpc.ErrMaybe
			}
			return reply.Err
		}
		retried = true
		time.Sleep(100 * time.Millisecond)
	}
}
