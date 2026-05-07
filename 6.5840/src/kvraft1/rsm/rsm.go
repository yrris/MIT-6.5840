package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// Op wraps a service request with a (peer, monotonic id) so the rsm
// reader can match committed entries with the Submit() that produced
// them. Field names must be capitalized for labgob.
type Op struct {
	Me  int
	Id  int64
	Req any
}

// StateMachine is the interface the wrapped service must implement.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type opResult struct {
	err rpc.Err
	rep any
}

type pending struct {
	op     Op
	result chan opResult
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int
	sm           StateMachine

	nextID  int64
	pending map[int]*pending
}

func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:      make(map[int]*pending),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	if snap := persister.ReadSnapshot(); len(snap) > 0 {
		rsm.sm.Restore(snap)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit hands req to Raft and waits for it to commit on this peer.
// Returns ErrWrongLeader if this peer is not (or stops being) leader.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	id := atomic.AddInt64(&rsm.nextID, 1)
	op := Op{Me: rsm.me, Id: id, Req: req}
	idx, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	ch := make(chan opResult, 1)
	if existing, ok := rsm.pending[idx]; ok {
		// A previous Submit at this index is still waiting; wake it up
		// — its op was displaced by ours under a new term.
		select {
		case existing.result <- opResult{err: rpc.ErrWrongLeader}:
		default:
		}
	}
	rsm.pending[idx] = &pending{op: op, result: ch}
	rsm.mu.Unlock()

	tk := time.NewTicker(20 * time.Millisecond)
	defer tk.Stop()
	for {
		select {
		case res := <-ch:
			return res.err, res.rep
		case <-tk.C:
			curTerm, stillLeader := rsm.rf.GetState()
			if stillLeader && curTerm == term {
				continue
			}
			rsm.mu.Lock()
			if cur, ok := rsm.pending[idx]; ok && cur.op.Id == op.Id && cur.op.Me == op.Me {
				delete(rsm.pending, idx)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
			// Reader already cleared our entry: drain the buffered result.
			select {
			case res := <-ch:
				return res.err, res.rep
			default:
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

// reader consumes applyCh, drives the state machine, and notifies any
// Submit waiting at each committed index. Exits when applyCh closes
// (i.e., raft has been Killed).
func (rsm *RSM) reader() {
	defer rsm.notifyAllPending()
	for m := range rsm.applyCh {
		switch {
		case m.SnapshotValid:
			rsm.handleSnapshot(m)
		case m.CommandValid:
			rsm.handleCommand(m)
		}
	}
}

func (rsm *RSM) handleSnapshot(m raftapi.ApplyMsg) {
	rsm.mu.Lock()
	rsm.sm.Restore(m.Snapshot)
	for idx, p := range rsm.pending {
		if idx <= m.SnapshotIndex {
			select {
			case p.result <- opResult{err: rpc.ErrWrongLeader}:
			default:
			}
			delete(rsm.pending, idx)
		}
	}
	rsm.mu.Unlock()
}

func (rsm *RSM) handleCommand(m raftapi.ApplyMsg) {
	op, ok := m.Command.(Op)
	if !ok {
		return
	}
	rep := rsm.sm.DoOp(op.Req)

	rsm.mu.Lock()
	if p, exists := rsm.pending[m.CommandIndex]; exists {
		var res opResult
		if p.op.Me == op.Me && p.op.Id == op.Id {
			res = opResult{err: rpc.OK, rep: rep}
		} else {
			res = opResult{err: rpc.ErrWrongLeader}
		}
		select {
		case p.result <- res:
		default:
		}
		delete(rsm.pending, m.CommandIndex)
	}

	if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
		data := rsm.sm.Snapshot()
		idx := m.CommandIndex
		rsm.mu.Unlock()
		rsm.rf.Snapshot(idx, data)
		return
	}
	rsm.mu.Unlock()
}

func (rsm *RSM) notifyAllPending() {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	for idx, p := range rsm.pending {
		select {
		case p.result <- opResult{err: rpc.ErrWrongLeader}:
		default:
		}
		delete(rsm.pending, idx)
	}
}
