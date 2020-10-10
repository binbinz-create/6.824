package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
const (
	MsgForTask = iota            // ask a task
	MsgForInterFileLoc			 //send intermediate file's localtion to master
	MsgForFinishMap				 //finsh a map task
	MsgForFinishReduce			// finsh a reduce task
)

// args struct
type MyArgs struct{
	MessageType int     //the value is the constant listed above
	MessageCnt string
}
// this args struct is the form send intermediate files' filename to master
type MyIntermediateFile struct {
	MessageType int
	MessageCnt string
	NReduceType int
}

//reply struct
type MyReply struct{
	FileName string
	MapNumAllocated int
	NReduce int
	ReduceNumAllocated int
	TaskType string    // refer a task type : "map" or "reduce"
	ReduceFileList []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
