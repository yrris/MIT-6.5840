package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

type byKey []KeyValue

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		if !call("Coordinator.GetTask", &args, &reply) {
			return // coordinator unreachable: job done.
		}

		switch reply.Kind {
		case KindMap:
			if doMap(reply.TaskID, reply.File, reply.NReduce, mapf) {
				reportDone(KindMap, reply.TaskID)
			}
		case KindReduce:
			if doReduce(reply.TaskID, reply.NMap, reducef) {
				reportDone(KindReduce, reply.TaskID)
			}
		case KindWait:
			time.Sleep(200 * time.Millisecond)
		case KindExit:
			return
		}
	}
}

// doMap reads the input file, partitions output into nReduce buckets,
// and atomically writes mr-<task>-<r> for each bucket.
func doMap(taskID int, file string, nReduce int, mapf func(string, string) []KeyValue) bool {
	content, err := os.ReadFile(file)
	if err != nil {
		return false
	}
	kva := mapf(file, string(content))

	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		buckets[r] = append(buckets[r], kv)
	}

	for r := 0; r < nReduce; r++ {
		tmp, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d-*", taskID, r))
		if err != nil {
			return false
		}
		enc := json.NewEncoder(tmp)
		failed := false
		for _, kv := range buckets[r] {
			if err := enc.Encode(&kv); err != nil {
				failed = true
				break
			}
		}
		tmp.Close()
		if failed {
			os.Remove(tmp.Name())
			return false
		}
		final := fmt.Sprintf("mr-%d-%d", taskID, r)
		if err := os.Rename(tmp.Name(), final); err != nil {
			os.Remove(tmp.Name())
			return false
		}
	}
	return true
}

// doReduce reads mr-<m>-<task> for every map task m, sorts, runs reducef
// per key, and atomically writes mr-out-<task>.
func doReduce(taskID int, nMap int, reducef func(string, []string) string) bool {
	var kva []KeyValue
	for m := 0; m < nMap; m++ {
		name := fmt.Sprintf("mr-%d-%d", m, taskID)
		f, err := os.Open(name)
		if err != nil {
			return false
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		f.Close()
	}
	sort.Sort(byKey(kva))

	tmp, err := os.CreateTemp(".", fmt.Sprintf("mr-out-%d-*", taskID))
	if err != nil {
		return false
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		out := reducef(kva[i].Key, values)
		fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, out)
		i = j
	}
	tmp.Close()
	final := fmt.Sprintf("mr-out-%d", taskID)
	if err := os.Rename(tmp.Name(), final); err != nil {
		os.Remove(tmp.Name())
		return false
	}
	return true
}

func reportDone(kind TaskKind, taskID int) {
	args := ReportTaskArgs{Kind: kind, TaskID: taskID}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// returns false if the coordinator cannot be reached or the call fails.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		return false
	}
	defer c.Close()
	if err := c.Call(rpcname, args, reply); err != nil {
		return false
	}
	return true
}
