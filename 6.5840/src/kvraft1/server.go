package kvraft

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type kvEntry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	mu   sync.Mutex
	data map[string]kvEntry
}

// DoOp runs in the rsm reader goroutine; serialized with respect to
// other DoOps and snapshot operations.
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch r := req.(type) {
	case rpc.GetArgs:
		if e, ok := kv.data[r.Key]; ok {
			return rpc.GetReply{Value: e.Value, Version: e.Version, Err: rpc.OK}
		}
		return rpc.GetReply{Err: rpc.ErrNoKey}
	case rpc.PutArgs:
		e, ok := kv.data[r.Key]
		if !ok {
			if r.Version == 0 {
				kv.data[r.Key] = kvEntry{Value: r.Value, Version: 1}
				return rpc.PutReply{Err: rpc.OK}
			}
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}
		if e.Version != r.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.data[r.Key] = kvEntry{Value: r.Value, Version: e.Version + 1}
		return rpc.PutReply{Err: rpc.OK}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
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
	var m map[string]kvEntry
	if err := d.Decode(&m); err == nil {
		kv.data = m
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, data: make(map[string]kvEntry)}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
