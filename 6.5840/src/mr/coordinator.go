package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// per-task state
type taskState int

const (
	stateIdle taskState = iota
	stateInProgress
	stateCompleted
)

type taskRecord struct {
	state taskState
	start time.Time
}

// job phase
type phase int

const (
	phaseMap phase = iota
	phaseReduce
	phaseDone
)

const taskTimeout = 10 * time.Second

type Coordinator struct {
	mu          sync.Mutex
	files       []string
	nMap        int
	nReduce     int
	mapTasks    []taskRecord
	reduceTasks []taskRecord
	phase       phase
}

// example handler kept for reference; not used by workers.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// GetTask hands an idle task to a worker, or tells it to wait/exit.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	if c.phase == phaseDone {
		reply.Kind = KindExit
		return nil
	}

	var tasks []taskRecord
	var kind TaskKind
	if c.phase == phaseMap {
		tasks = c.mapTasks
		kind = KindMap
	} else {
		tasks = c.reduceTasks
		kind = KindReduce
	}

	for i := range tasks {
		if tasks[i].state == stateIdle {
			tasks[i].state = stateInProgress
			tasks[i].start = time.Now()
			reply.Kind = kind
			reply.TaskID = i
			if c.phase == phaseMap {
				reply.File = c.files[i]
			}
			return nil
		}
	}

	// no idle task in current phase: tell worker to wait.
	reply.Kind = KindWait
	return nil
}

// ReportTask marks the given task completed; stale reports are ignored.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var tasks []taskRecord
	switch args.Kind {
	case KindMap:
		if c.phase != phaseMap {
			return nil
		}
		tasks = c.mapTasks
	case KindReduce:
		if c.phase != phaseReduce {
			return nil
		}
		tasks = c.reduceTasks
	default:
		return nil
	}
	if args.TaskID < 0 || args.TaskID >= len(tasks) {
		return nil
	}
	if tasks[args.TaskID].state == stateInProgress {
		tasks[args.TaskID].state = stateCompleted
	}
	c.advancePhase()
	return nil
}

// advancePhase walks Map -> Reduce -> Done as tasks complete.
// caller must hold c.mu.
func (c *Coordinator) advancePhase() {
	if c.phase == phaseMap {
		for _, t := range c.mapTasks {
			if t.state != stateCompleted {
				return
			}
		}
		c.phase = phaseReduce
	}
	if c.phase == phaseReduce {
		for _, t := range c.reduceTasks {
			if t.state != stateCompleted {
				return
			}
		}
		c.phase = phaseDone
	}
}

// reaper resets in-progress tasks whose worker has been silent too long.
func (c *Coordinator) reaper() {
	for {
		time.Sleep(time.Second)
		c.mu.Lock()
		if c.phase == phaseDone {
			c.mu.Unlock()
			return
		}
		var tasks []taskRecord
		if c.phase == phaseMap {
			tasks = c.mapTasks
		} else {
			tasks = c.reduceTasks
		}
		now := time.Now()
		for i := range tasks {
			if tasks[i].state == stateInProgress && now.Sub(tasks[i].start) > taskTimeout {
				tasks[i].state = stateIdle
			}
		}
		c.mu.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == phaseDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nMap:        len(files),
		nReduce:     nReduce,
		mapTasks:    make([]taskRecord, len(files)),
		reduceTasks: make([]taskRecord, nReduce),
		phase:       phaseMap,
	}
	go c.reaper()
	c.server(sockname)
	return &c
}
