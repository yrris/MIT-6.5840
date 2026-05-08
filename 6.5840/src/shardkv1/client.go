package shardkv

// Top-level shardkv clerk: discovers the owning shardgrp via the
// shardctrler and routes Get/Put to it. Caches the most recent
// configuration so repeated Get/Put on stable config does just one RPC
// to kvsrv per cache miss; refreshes on ErrWrongGroup / ErrWrongLeader.

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler

	mu   sync.Mutex
	rcks map[tester.Tgid]*shardgrp.Clerk
	cfg  *shardcfg.ShardConfig // cached; nil means "must Query"
}

func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	return &Clerk{
		clnt: clnt,
		sck:  sck,
		rcks: make(map[tester.Tgid]*shardgrp.Clerk),
	}
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// groupClerk returns a (cached) shardgrp.Clerk for `gid`. We cache so
// repeated requests to the same group reuse the leader hint.
func (ck *Clerk) groupClerk(gid tester.Tgid, srvs []string) *shardgrp.Clerk {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if c, ok := ck.rcks[gid]; ok {
		return c
	}
	c := shardgrp.MakeClerk(ck.clnt, srvs)
	ck.rcks[gid] = c
	return c
}

// loadConfig returns the cached config, querying the shardctrler only
// when necessary. force=true bypasses the cache (used after a stale
// routing decision returned ErrWrongGroup / ErrWrongLeader).
func (ck *Clerk) loadConfig(force bool) *shardcfg.ShardConfig {
	if !force {
		ck.mu.Lock()
		c := ck.cfg
		ck.mu.Unlock()
		if c != nil {
			return c
		}
	}
	c := ck.sck.Query()
	ck.mu.Lock()
	ck.cfg = c
	ck.mu.Unlock()
	return c
}

// Get loops over (load config → contact owning group). ErrWrongGroup /
// ErrWrongLeader from the inner clerk both kick us back to a fresh
// Query so reconfiguration and group death are transparent.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	shard := shardcfg.Key2Shard(key)
	force := false
	for {
		cfg := ck.loadConfig(force)
		force = true // any subsequent iteration must refresh
		gid, srvs, ok := cfg.GidServers(shard)
		if ok && gid != 0 && len(srvs) > 0 {
			rck := ck.groupClerk(gid, srvs)
			v, ver, err := rck.Get(key)
			if err != rpc.ErrWrongGroup && err != rpc.ErrWrongLeader {
				return v, ver, err
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Put propagates whatever the owning shardgrp returns, with one
// nuance: if we ever bailed out of the inner Put with ErrWrongLeader
// (cached group exhausted its retry budget), the original request may
// have committed in raft and only the reply was lost. A subsequent
// ErrVersion on the same args could therefore be from our own earlier
// success — translate it to ErrMaybe per lab spec. ErrWrongGroup, by
// contrast, means the shardgrp's DoOp explicitly rejected us, so we
// know nothing applied at that hop and don't need to set the flag.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	shard := shardcfg.Key2Shard(key)
	retried := false
	force := false
	for {
		cfg := ck.loadConfig(force)
		force = true
		gid, srvs, ok := cfg.GidServers(shard)
		if ok && gid != 0 && len(srvs) > 0 {
			rck := ck.groupClerk(gid, srvs)
			err := rck.Put(key, value, version)
			switch err {
			case rpc.ErrWrongLeader:
				retried = true
			case rpc.ErrWrongGroup:
				// Definitely didn't apply at this group; re-route.
			case rpc.ErrVersion:
				if retried {
					return rpc.ErrMaybe
				}
				return err
			default:
				return err
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}
