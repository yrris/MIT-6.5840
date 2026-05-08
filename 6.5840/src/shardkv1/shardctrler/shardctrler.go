package shardctrler

// Sharded-key/value controller. State (current and pending "next"
// configurations) lives in a single-server kvsrv1 KV.
//
// Coordination model — relies on the fact that kvsrv1 Put is a CAS on
// (key, version):
//
//   * Two keys are stored in the kvsrv: keyCurrent and keyNext.
//   * InitConfig seeds both with the bootstrap config (Num=1).
//   * ChangeConfigTo:
//       1. Reads (current, next).
//       2. If next.Num > current.Num the previous controller died
//          mid-reconfig: re-issue the moves (idempotent on shardgrps,
//          fenced by per-shard Num) and CAS-update keyCurrent to next.
//       3. If next.Num == current.Num try to CAS keyNext from the old
//          version to our `new` config; the loser of the race just
//          loops and helps finish whoever won.
//       4. After our own moves succeed, CAS-update keyCurrent.
//   * InitController is the recovery hook the tester calls when a new
//     controller process starts: same step 2 logic in isolation.
//
// 5C concurrency: the CAS on keyNext is the single serializing point
// across racing controllers. Once one wins, every other controller
// either (a) sees current ≥ their target and exits, or (b) helps
// complete the winner's reconfig. Stale RPCs from a partitioned
// controller are filtered out by the shardgrp Num fence.
//
// Bounded retries: every kvsrv1 Get/Put goes through bounded helpers
// that give up after `kvSweeps`; this keeps a controller goroutine
// orphaned on a torn-down test network from spinning forever and
// dragging the whole process to a halt across tests.

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

const (
	keyCurrent = "shardctrler/current"
	keyNext    = "shardctrler/next"
)

// kvSweeps bounds how many full retry rounds we wait on each kvsrv1
// call before giving up. Each sweep sleeps 100ms after a no-reply, so
// the upper bound is ~kvSweeps * 100ms wall time. Tuned low so a
// controller goroutine orphaned on a torn-down test network exits
// quickly instead of spawning more daemon groups every iteration —
// this is the cumulative-resource killer for `make shardkv` runs.
const kvSweeps = 5

type ShardCtrler struct {
	clnt   *tester.Clnt
	server string

	kvtest.IKVClerk

	killed int32 // set by Kill()

	// cacheMu guards cachedCfg, the last cfg we successfully read from
	// keyCurrent. Returning a stale cfg (instead of an empty one) when
	// the clnt is partitioned keeps test helpers like ts.leave from
	// hitting `Leave(...) but not in config` log.Fatalf.
	cacheMu   sync.Mutex
	cachedCfg *shardcfg.ShardConfig
}

func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	srv := tester.ServerName(tester.GRP0, 0)
	return &ShardCtrler{
		clnt:     clnt,
		server:   srv,
		IKVClerk: kvsrv.MakeClerk(clnt, srv),
	}
}

// kvGet is a bounded variant of kvsrv1.Clerk.Get used internally by
// the controller. The standard Clerk retries forever on missing
// replies, which makes a goroutine that survives test cleanup spin
// indefinitely; we cap retries so a dead network surfaces as
// ErrWrongLeader and lets ChangeConfigTo unwind gracefully.
func (sck *ShardCtrler) kvGet(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for sweep := 0; sweep < kvSweeps && !sck.IsKilled(); sweep++ {
		reply := rpc.GetReply{}
		if sck.clnt.Call(sck.server, "KVServer.Get", &args, &reply) {
			return reply.Value, reply.Version, reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", 0, rpc.ErrWrongLeader
}

// kvPut is a bounded variant of kvsrv1.Clerk.Put with the same
// ErrMaybe semantics as the standard clerk.
func (sck *ShardCtrler) kvPut(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	retried := false
	for sweep := 0; sweep < kvSweeps && !sck.IsKilled(); sweep++ {
		reply := rpc.PutReply{}
		if sck.clnt.Call(sck.server, "KVServer.Put", &args, &reply) {
			if reply.Err == rpc.ErrVersion && retried {
				return rpc.ErrMaybe
			}
			return reply.Err
		}
		retried = true
		time.Sleep(100 * time.Millisecond)
	}
	return rpc.ErrWrongLeader
}

// InitController is called on every fresh controller process. If we
// were the sole controller this is a no-op; otherwise we may be the
// recovery controller for a previous one that died mid-reconfig.
func (sck *ShardCtrler) InitController() {
	curS, vCur, errC := sck.kvGet(keyCurrent)
	nxtS, _, errN := sck.kvGet(keyNext)
	if errC != rpc.OK || errN != rpc.OK {
		return
	}
	cur := shardcfg.FromString(curS)
	nxt := shardcfg.FromString(nxtS)
	if nxt.Num > cur.Num {
		sck.applyMoves(cur, nxt)
		// CAS keyCurrent to advance. If someone else beat us to it
		// that's fine — both ended up at the same value.
		sck.kvPut(keyCurrent, nxtS, vCur)
	}
}

// InitConfig seeds the two keys at version 0 (creates them).
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	s := cfg.String()
	sck.kvPut(keyCurrent, s, 0)
	sck.kvPut(keyNext, s, 0)
}

// ChangeConfigTo drives the system from the current configuration to
// `new`. Loops until either the system has caught up (cur.Num >=
// new.Num) or our advance has been preempted by another controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for !sck.IsKilled() {
		curS, vCur, errC := sck.kvGet(keyCurrent)
		nxtS, vNxt, errN := sck.kvGet(keyNext)
		if errC != rpc.OK || errN != rpc.OK {
			return // bounded helper gave up; nothing more we can do
		}
		cur := shardcfg.FromString(curS)
		nxt := shardcfg.FromString(nxtS)

		// Someone (possibly us in an earlier round) already advanced
		// past our target.
		if cur.Num >= new.Num {
			return
		}

		// A previous reconfig is still in flight — finish it before
		// proposing our own.
		if nxt.Num > cur.Num {
			sck.applyMoves(cur, nxt)
			sck.kvPut(keyCurrent, nxtS, vCur)
			continue
		}

		// nxt.Num == cur.Num: race for the next slot.
		err := sck.kvPut(keyNext, new.String(), vNxt)
		if err == rpc.OK {
			sck.applyMoves(cur, new)
			sck.kvPut(keyCurrent, new.String(), vCur)
			return
		}
		// ErrVersion / ErrMaybe / anything else: loop and re-evaluate.
		// The next iteration will see whoever's value actually landed
		// in keyNext and either help them finish or notice we're done.
	}
}

// Query returns the current configuration. When the underlying clnt
// has been partitioned away (kvGet exhausted retries) we return the
// last cfg we successfully read instead of an empty one — test
// helpers like `ts.leave` would otherwise treat the empty cfg as
// "group already gone" and log.Fatalf, even though the real cfg in
// kvsrv still has the group.
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	for !sck.IsKilled() {
		s, _, err := sck.kvGet(keyCurrent)
		if err == rpc.OK {
			cfg := shardcfg.FromString(s)
			sck.cacheMu.Lock()
			sck.cachedCfg = cfg
			sck.cacheMu.Unlock()
			return cfg
		}
		if err == rpc.ErrWrongLeader {
			sck.cacheMu.Lock()
			cached := sck.cachedCfg
			sck.cacheMu.Unlock()
			if cached != nil {
				return cached.Copy()
			}
			return shardcfg.MakeShardConfig()
		}
		time.Sleep(50 * time.Millisecond)
	}
	return shardcfg.MakeShardConfig()
}

// applyMoves walks the shard map and issues Freeze → Install → Delete
// for each shard whose owner changes between old and new. Per-shard Num
// fencing on the shardgrp side makes every individual RPC idempotent
// across re-issues.
func (sck *ShardCtrler) applyMoves(old, new *shardcfg.ShardConfig) {
	for s := 0; s < shardcfg.NShards; s++ {
		if sck.IsKilled() {
			return
		}
		oldGid, newGid := old.Shards[s], new.Shards[s]
		if oldGid == newGid {
			continue
		}
		var data []byte
		if oldGid != 0 {
			if srvs, ok := old.Groups[oldGid]; ok && len(srvs) > 0 {
				ck := shardgrp.MakeClerk(sck.clnt, srvs)
				d, err := ck.FreezeShard(shardcfg.Tshid(s), new.Num)
				if err == rpc.OK {
					data = d
				}
			}
		}
		if newGid != 0 {
			if srvs, ok := new.Groups[newGid]; ok && len(srvs) > 0 {
				ck := shardgrp.MakeClerk(sck.clnt, srvs)
				ck.InstallShard(shardcfg.Tshid(s), data, new.Num)
			}
		}
		if oldGid != 0 {
			if srvs, ok := old.Groups[oldGid]; ok && len(srvs) > 0 {
				ck := shardgrp.MakeClerk(sck.clnt, srvs)
				ck.DeleteShard(shardcfg.Tshid(s), new.Num)
			}
		}
	}
}

// IsKilled lets long-running loops abort when the test reaper sets it.
func (sck *ShardCtrler) IsKilled() bool {
	return atomic.LoadInt32(&sck.killed) != 0
}

func (sck *ShardCtrler) Kill() {
	atomic.StoreInt32(&sck.killed, 1)
}
