package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Task struct {
	TaskId   int
	TaskType string
	File     string
}

type Coordinator struct {
	// Your definitions here.
	Chan            chan Task
	mu              sync.Mutex
	RChan           chan int
	mapDoneCount    int
	mapCount        int
	reduceDoneCount int
	nReduce         int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ReportDone(args *ReportDoneArgs, reply *ReportDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskType := args.TaskType

	if taskType == "map" {
		c.mapDoneCount++
	}

	if taskType == "reduce" {
		c.reduceDoneCount++
	}

	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	reply.Task = nil
	reply.Done = false
	reply.NReduce = c.nReduce

	// Try to get a map task first
	select {
	case task, ok := <-c.Chan:
		if ok {
			reply.Task = &task
			return nil
		}
	default:
	}

	// If no map tasks available, check if all maps are done
	c.mu.Lock()
	allMapsDone := c.mapDoneCount >= c.mapCount
	c.mu.Unlock()

	if !allMapsDone {
		// Maps still in progress - tell worker to wait
		return nil
	}

	// All maps done - initialize reduce tasks if needed
	c.mu.Lock()
	if c.RChan == nil {
		c.RChan = make(chan int, c.nReduce)
		for i := 0; i < c.nReduce; i++ {
			c.RChan <- i
		}
		close(c.RChan)
	}
	c.mu.Unlock()

	// Try to get a reduce task
	select {
	case tskNum, ok := <-c.RChan:
		if ok {
			reply.Task = &Task{
				TaskId:   tskNum,
				TaskType: "reduce",
			}
			return nil
		}
	default:
	}

	// Check if all work is done
	c.mu.Lock()
	done := c.reduceDoneCount >= c.nReduce
	c.mu.Unlock()

	if done {
		reply.Done = true
	}

	return nil
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
	ret := false
	c.mu.Lock()
	// Your code here.
	defer c.mu.Unlock()

	if c.reduceDoneCount == c.nReduce {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapCount:        len(files),
		mapDoneCount:    0,
		reduceDoneCount: 0,
		nReduce:         nReduce,
	}

	// Your code here.

	c.Chan = make(chan Task, len(files))
	if len(files) < nReduce {
		c.Chan = make(chan Task, nReduce)
	}

	for i, file := range files {
		c.Chan <- Task{
			TaskId:   i,
			File:     file,
			TaskType: "map",
		}
	}

	close(c.Chan)
	sockname := "sock123"
	c.server(sockname)
	return &c
}
