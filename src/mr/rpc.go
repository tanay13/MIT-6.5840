package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type GetTaskArgs struct{}

type GetTaskReply struct {
	Task    *Task
	NReduce int
	Done    bool
}

type ReportDoneArgs struct {
	TaskType string
}

type ReportDoneReply struct{}
