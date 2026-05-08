package shardgrp

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

// kvEntry is the per-key value/version tuple, identical to lab2/lab4.
type kvEntry struct {
	Value   string
	Version rpc.Tversion
}

// Phase tracks each shard's lifecycle relative to reconfiguration.
type Phase int

const (
	PhaseLive   Phase = iota // owned by this group; serves Get/Put.
	PhaseFrozen              // owned, but in transit; rejects Get/Put with ErrWrongGroup.
	PhaseGone                // not owned; Freeze/Install/Delete may move it back.
)

// shardState is the entire per-shard replicated state.
//
// Num is the largest config-Num seen in any FreezeShard / InstallShard /
// DeleteShard RPC for this shard. It fences out stale RPCs from old
// controllers so they don't clobber state that has already moved on.
type shardState struct {
	Data  map[string]kvEntry
	Phase Phase
	Num   shardcfg.Tnum
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	mu     sync.Mutex
	shards [shardcfg.NShards]shardState
}

// DoOp runs in the rsm reader goroutine, so it is serialized with
// other DoOps and with Snapshot/Restore.
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch r := req.(type) {
	case rpc.GetArgs:
		return kv.applyGet(r)
	case rpc.PutArgs:
		return kv.applyPut(r)
	case shardrpc.FreezeShardArgs:
		return kv.applyFreeze(r)
	case shardrpc.InstallShardArgs:
		return kv.applyInstall(r)
	case shardrpc.DeleteShardArgs:
		return kv.applyDelete(r)
	}
	return nil
}

func (kv *KVServer) applyGet(args rpc.GetArgs) rpc.GetReply {
	s := &kv.shards[shardcfg.Key2Shard(args.Key)]
	if s.Phase != PhaseLive {
		return rpc.GetReply{Err: rpc.ErrWrongGroup}
	}
	if e, ok := s.Data[args.Key]; ok {
		return rpc.GetReply{Value: e.Value, Version: e.Version, Err: rpc.OK}
	}
	return rpc.GetReply{Err: rpc.ErrNoKey}
}

func (kv *KVServer) applyPut(args rpc.PutArgs) rpc.PutReply {
	s := &kv.shards[shardcfg.Key2Shard(args.Key)]
	if s.Phase != PhaseLive {
		return rpc.PutReply{Err: rpc.ErrWrongGroup}
	}
	e, ok := s.Data[args.Key]
	if !ok {
		if args.Version == 0 {
			s.Data[args.Key] = kvEntry{Value: args.Value, Version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		return rpc.PutReply{Err: rpc.ErrNoKey}
	}
	if e.Version != args.Version {
		return rpc.PutReply{Err: rpc.ErrVersion}
	}
	s.Data[args.Key] = kvEntry{Value: args.Value, Version: e.Version + 1}
	return rpc.PutReply{Err: rpc.OK}
}

// applyFreeze: source-side handler. After Freeze the shard is owned but
// rejects Get/Put; the data is bundled into the reply so the controller
// can ship it to the destination group.
func (kv *KVServer) applyFreeze(args shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	s := &kv.shards[args.Shard]
	if args.Num < s.Num {
		return shardrpc.FreezeShardReply{Err: rpc.ErrVersion, Num: s.Num}
	}
	// args.Num >= s.Num. Move into Frozen if we still own data; if we
	// already advanced past Freeze in this Num cycle (e.g. Delete already
	// applied) the reply just carries whatever (possibly empty) data we
	// have, which is safe because the destination's Install at this Num
	// will be idempotent.
	if args.Num > s.Num || s.Phase == PhaseLive {
		s.Phase = PhaseFrozen
		s.Num = args.Num
	}
	return shardrpc.FreezeShardReply{
		State: encodeShardData(s.Data),
		Num:   s.Num,
		Err:   rpc.OK,
	}
}

// applyInstall: destination-side handler. Replaces shard contents and
// flips Phase to Live.
func (kv *KVServer) applyInstall(args shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	s := &kv.shards[args.Shard]
	if args.Num < s.Num {
		return shardrpc.InstallShardReply{Err: rpc.ErrVersion}
	}
	// args.Num == s.Num and we're already Live: idempotent retry, keep
	// existing data. Otherwise install (also covers the args.Num > s.Num
	// case).
	if args.Num > s.Num || s.Phase != PhaseLive {
		s.Data = decodeShardData(args.State)
		s.Phase = PhaseLive
		s.Num = args.Num
	}
	return shardrpc.InstallShardReply{Err: rpc.OK}
}

// applyDelete: source-side cleanup after Install succeeded on the
// destination.
func (kv *KVServer) applyDelete(args shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	s := &kv.shards[args.Shard]
	if args.Num < s.Num {
		return shardrpc.DeleteShardReply{Err: rpc.ErrVersion}
	}
	if args.Num > s.Num || s.Phase != PhaseGone {
		s.Data = nil
		s.Phase = PhaseGone
		s.Num = args.Num
	}
	return shardrpc.DeleteShardReply{Err: rpc.OK}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shards [shardcfg.NShards]shardState
	if err := d.Decode(&shards); err == nil {
		kv.shards = shards
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.FreezeShardReply)
}

func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.InstallShardReply)
}

func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.DeleteShardReply)
}

// encodeShardData copies the live map into a self-contained byte buffer.
// The lab spec warns that returning a live map in an RPC reply races
// with concurrent state machine writes; the gob encoder bytes are an
// immutable snapshot, so they're safe to ship.
func encodeShardData(m map[string]kvEntry) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if m == nil {
		m = map[string]kvEntry{}
	}
	e.Encode(m)
	return w.Bytes()
}

func decodeShardData(data []byte) map[string]kvEntry {
	if len(data) == 0 {
		return map[string]kvEntry{}
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var m map[string]kvEntry
	if err := d.Decode(&m); err != nil || m == nil {
		return map[string]kvEntry{}
	}
	return m
}

// StartServerShardGrp starts a server for shardgrp `gid`.
//
// gid == shardcfg.Gid1 owns all shards from boot per the lab spec
// ("Upon creation, the first shardgrp should initialize itself to own
// all shards"); other groups boot with every shard in PhaseGone.
//
// StartServerShardGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	for s := 0; s < shardcfg.NShards; s++ {
		if gid == shardcfg.Gid1 {
			kv.shards[s] = shardState{Data: map[string]kvEntry{}, Phase: PhaseLive}
		} else {
			kv.shards[s] = shardState{Phase: PhaseGone}
		}
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
