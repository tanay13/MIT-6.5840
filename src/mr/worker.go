package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	coordSockName = "sock123"
	for {
		tsk := CallGetTask()

		if tsk == nil || tsk.Done {
			return
		}

		if tsk.Task == nil {
			// No task available - sleep and retry
			time.Sleep(100 * time.Millisecond)
			continue
		}

		switch tsk.Task.TaskType {
		case "map":
			mapWork(tsk, mapf)
			CallReportDone("map")
		case "reduce":
			reduceWork(tsk, reducef)
			CallReportDone("reduce")
		}
	}
}

func reduceWork(task *GetTaskReply, reducef func(string, []string) string) {
	bucket := task.Task.TaskId
	pattern := fmt.Sprintf("M-*-%d", bucket)
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal(err)
	}

	intermediate := []KeyValue{}

	for _, fname := range files {
		file, err := os.Open(fname)
		if err != nil {
			log.Fatal(err)
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err)
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	grouped := make(map[string][]string)

	for _, kv := range intermediate {
		grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
	}

	oname := fmt.Sprintf("mr-out-%d", bucket)
	ofile, _ := os.Create(oname)

	defer ofile.Close()

	for k, values := range grouped {
		time.Sleep(10 * time.Millisecond)
		output := reducef(k, values)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
}

func mapWork(task *GetTaskReply, mapf func(string, string) []KeyValue) {
	filename := task.Task.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	files := make(map[int]*os.File)
	encoders := make(map[int]*json.Encoder)

	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReduce

		if _, ok := files[r]; !ok {
			fileName := fmt.Sprintf("M-%d-%d", task.Task.TaskId, r)

			f, err := os.Create(fileName)
			if err != nil {
				log.Fatal(err)
			}

			files[r] = f
			encoders[r] = json.NewEncoder(f)
		}

		if err := encoders[r].Encode(kv); err != nil {
			log.Fatal(err)
		}
	}

	for _, f := range files {
		f.Close()
	}
}

func CallReportDone(taskType string) {
	args := ReportDoneArgs{}
	reply := ReportDoneReply{}
	args.TaskType = taskType
	_ = call("Coordinator.ReportDone", &args, &reply)
}

func CallGetTask() *GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		return nil
	}
	return &reply
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
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
