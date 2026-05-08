package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

// clientSweeps bounds Get/Put retry cycles inside this clerk so the
// caller (shardkv1.Clerk) gets a chance to re-Query the shardctrler when
// a group has gone away (e.g. after `leaveGroups + ExitGroup`). Each
// sweep visits every server once with sleepBetweenSweeps in between.
const (
	clientSweeps        = 10
	sleepBetweenSweeps  = 100 * time.Millisecond
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // index of last successful leader; tried first next time.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	return &Clerk{Clnt: clnt, servers: servers}
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get tries each server in turn (starting from the cached leader) until
// somebody answers with anything other than ErrWrongLeader. After
// `clientSweeps` full passes with no usable answer, gives up with
// ErrWrongLeader so the caller can re-Query.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for sweep := 0; sweep < clientSweeps; sweep++ {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := rpc.GetReply{}
			ok := ck.Call(ck.servers[srv], "KVServer.Get", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			ck.leader = srv
			return reply.Value, reply.Version, reply.Err
		}
		time.Sleep(sleepBetweenSweeps)
	}
	return "", 0, rpc.ErrWrongLeader
}

// Put preserves the lab2/lab4 ErrMaybe semantics: once we've sent more
// than one request, an ErrVersion reply could be due to our own earlier
// (lost-reply) success, so we downgrade to ErrMaybe. Bounded by
// `clientSweeps` for the same reason as Get.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	retried := false
	for sweep := 0; sweep < clientSweeps; sweep++ {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := rpc.PutReply{}
			ok := ck.Call(ck.servers[srv], "KVServer.Put", &args, &reply)
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
		time.Sleep(sleepBetweenSweeps)
	}
	return rpc.ErrWrongLeader
}

// controlSweeps is the maximum number of full server-sweeps the
// control-plane RPCs perform before returning ErrWrongLeader. Long
// enough to weather lab 5B's "shardgrp is shut down for a couple of
// seconds and then comes back" scenario, but short enough that an
// orphaned controller goroutine on a torn-down test network exits
// instead of spinning forever.
const controlSweeps = 30

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	for sweep := 0; sweep < controlSweeps; sweep++ {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := shardrpc.FreezeShardReply{}
			ok := ck.Call(ck.servers[srv], "KVServer.FreezeShard", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			ck.leader = srv
			return reply.State, reply.Err
		}
		time.Sleep(sleepBetweenSweeps)
	}
	return nil, rpc.ErrWrongLeader
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	for sweep := 0; sweep < controlSweeps; sweep++ {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := shardrpc.InstallShardReply{}
			ok := ck.Call(ck.servers[srv], "KVServer.InstallShard", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			ck.leader = srv
			return reply.Err
		}
		time.Sleep(sleepBetweenSweeps)
	}
	return rpc.ErrWrongLeader
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	for sweep := 0; sweep < controlSweeps; sweep++ {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := shardrpc.DeleteShardReply{}
			ok := ck.Call(ck.servers[srv], "KVServer.DeleteShard", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			ck.leader = srv
			return reply.Err
		}
		time.Sleep(sleepBetweenSweeps)
	}
	return rpc.ErrWrongLeader
}
