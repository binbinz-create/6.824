package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	AllFilesName  map[string]int //split files  ,  the identifier of the map task can be represented by the input file name
	MapTaskNumber int            //current map task number
	NReduce       int            // n reduces task
	//
	InterFileName    [][]string  //store location of the intermediate file name
	ReduceTaskStatus map[int]int //about reduce task's status  , the identifier of the reduce task can by represented by the number
	MapFinished      bool
	ReduceFinished   bool
	RWLock           sync.RWMutex
}

//task's status
//UnAllocated   -->  Unallocated to any woker
//Allocated     -->  be allocated to a worker
//Finished      -->  woker finish the map task
const (
	Unallocated = iota
	Allocated
	Finished
)

var maptasks chan string // chan for map task
var reducetasks chan int // chan for reduce task

//generate the map task and reduce task
func (m *Master) generateTask() {

	for k, v := range m.AllFilesName {
		if v == Unallocated {
			maptasks <- k    //一次只能装进去5个，要等woker运行掉一些任务以后才可以继续放入到maptasks中
		}
	}

	ok := false
	for !ok {
		ok = checkAllMapTasks(m)
	}

	m.MapFinished = true

	for k, v := range m.ReduceTaskStatus {
		if v == Unallocated {
			reducetasks <- k
		}
	}


	ok = false
	for !ok {
		ok = checkAllReduceTasks(m)
	}

	m.ReduceFinished = true

}

//check if all map tasks are finished
func checkAllMapTasks(m *Master) bool {
	m.RWLock.RLock()
	defer m.RWLock.RUnlock()
	for _, v := range m.AllFilesName {
		if v != Finished {
			return false
		}
	}
	return true
}

//check if all reduce tasks are finished
func checkAllReduceTasks(m *Master) bool {
	m.RWLock.RLock()
	defer m.RWLock.RUnlock()
	for _, v := range m.ReduceTaskStatus {
		if v != Finished {
			return false
		}
	}
	return true
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) MyCallHandler(args *MyArgs, reply *MyReply) error {
	msgType := args.MessageType
	switch msgType {
	case MsgForTask:
		select {
		case filename := <-maptasks:
			//allocate map task
			reply.TaskType = "map"
			reply.FileName = filename
			reply.MapNumAllocated = m.MapTaskNumber
			reply.NReduce = m.NReduce

			m.RWLock.Lock()
			m.AllFilesName[filename] = Allocated
			m.MapTaskNumber++
			m.RWLock.Unlock()

			go m.timeForWorker("map",filename)

			return nil
		case reduceNum := <-reducetasks:
			//allocate reduce task
			reply.TaskType = "reduce"
			reply.NReduce = m.NReduce
			reply.ReduceNumAllocated = reduceNum
			reply.ReduceFileList = m.InterFileName[reduceNum] //the intermediate file location corresponding to this reduce

			m.RWLock.Lock()
			m.ReduceTaskStatus[reduceNum] = Allocated
			m.RWLock.Unlock()

			go m.timeForWorker("reduce",strconv.Itoa(reduceNum))

			return nil
		}
	case MsgForFinishMap:
		//finish a map task
		m.RWLock.Lock()
		defer m.RWLock.Unlock()
		m.AllFilesName[args.MessageCnt] = Finished
	case MsgForFinishReduce:
		//finish a reduce task
		index, _ := strconv.Atoi(args.MessageCnt)
		m.RWLock.Lock()
		defer m.RWLock.Unlock()
		m.ReduceTaskStatus[index] = Finished
	}

	return nil
}

// intermediate file's handler
func (m *Master) MyInnerFileHandler(args *MyIntermediateFile, reply *MyReply) error {

	nReduceNum := args.NReduceType //在shuffle阶段就指定了中间文件由哪一个reduce进行处理？？？
	fileName := args.MessageCnt

	//store them
	m.RWLock.Lock()
	defer m.RWLock.Unlock()
	m.InterFileName[nReduceNum] = append(m.InterFileName[nReduceNum], fileName)

	return nil
}

//monitor the worker
func (m *Master) timeForWorker(taskType, identity string) {
	ticker := time.NewTicker(10 * time.Second) // A tick is sent to the channel after ten seconds
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C: //if there is time in the channel , it means that task has not been completed after ten seconds. Think the task failed and redistribute it
			if taskType == "map" {
				m.RWLock.Lock()
				m.AllFilesName[identity] = Unallocated
				m.RWLock.Unlock()
				//Re-add the map task to the channel
				maptasks <- identity
			} else if taskType == "reduce" {
				index, _ := strconv.Atoi(identity)
				m.RWLock.Lock()
				m.ReduceTaskStatus[index] = Unallocated
				m.RWLock.Unlock()
				//Re-add the reduce task to the channel
				reducetasks <- index
			}
			return
		default:
			if taskType == "map"{
				m.RWLock.RLock()
				if m.AllFilesName[identity] == Finished{   //judge if the map task has been completed
					m.RWLock.RUnlock()
					return
				}
				m.RWLock.RUnlock()
			}else if taskType == "reduce"{
				index , _ := strconv.Atoi(identity)
				m.RWLock.RLock()
				if m.ReduceTaskStatus[index] == Finished{ //judge if the reduce map task has been completed
					m.RWLock.RUnlock()
					return
				}
				m.RWLock.RUnlock()
			}
		}
	}

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	//init the channel
	maptasks = make(chan string, 5)
	reducetasks = make(chan int, 5)

	rpc.Register(m)
	rpc.HandleHTTP()

	//parallel run the map task
	go m.generateTask()

	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = m.ReduceFinished

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	//init master struct (m)
	m.AllFilesName = make(map[string]int)
	m.MapTaskNumber = 0
	m.NReduce = nReduce
	m.InterFileName = make([][]string, nReduce)
	m.ReduceTaskStatus = make(map[int]int)
	m.MapFinished = false
	m.ReduceFinished = false

	for _, v := range files {
		m.AllFilesName[v] = Unallocated
	}


	for i := 0; i < nReduce; i++ {
		m.ReduceTaskStatus[i] = Unallocated
	}

	m.server()
	return &m
}
