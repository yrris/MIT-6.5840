package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// task kinds the coordinator hands to workers.
type TaskKind int

const (
	KindMap TaskKind = iota
	KindReduce
	KindWait
	KindExit
)

type GetTaskArgs struct{}

type GetTaskReply struct {
	Kind    TaskKind
	TaskID  int
	File    string // input file (Map only)
	NReduce int
	NMap    int
}

type ReportTaskArgs struct {
	Kind   TaskKind
	TaskID int
}

type ReportTaskReply struct{}
