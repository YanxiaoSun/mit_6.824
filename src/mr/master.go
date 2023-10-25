package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


type Master struct {
	// Your definitions here.
	mrecord []int // 0表示未执行，1表示正在执行，2表示执行完成
	rrecord []int
	Nmap int
	Nreduce int
	files []string

	mu sync.Mutex
	Mcount int
	Rcount int

}
const (
	NotStarted = iota
	Processing
	Finished
)

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTask(args *WorkerArgs, reply *WorkerReply) error{
	m.mu.Lock()
	defer m.mu.Unlock()
	// map task
	if m.Mcount < m.Nmap{
		for k := range m.mrecord{
			if m.mrecord[k] == NotStarted{
				reply.Filename = m.files[k]
				reply.TaskType = 0
				reply.MapTaskId = k
				m.mrecord[k] = Processing
				reply.Nreduce  = m.Nreduce
				//m.mu.Unlock()
				go m.HandleTimeOut(reply)
				return nil
			}
		}
		reply.TaskType = 2
		//m.mu.Unlock()
		return nil
	// reduce task
	}else if m.Mcount == m.Nmap && m.Rcount < m.Nreduce{
		for k := range m.rrecord{
			if m.rrecord[k] == NotStarted{
				reply.Nmap = m.Nmap
				reply.TaskType = 1
				m.rrecord[k] = Processing
				reply.ReduceTaskId = k
				go m.HandleTimeOut(reply)
				return nil
			}
		}
		reply.TaskType = 2
		return nil
	}else{
		reply.TaskType = 3
		return nil
	}
}



func (m *Master) HandleTimeOut(reply *WorkerReply){
	time.Sleep(time.Second * 10)
	m.mu.Lock()
	defer m.mu.Unlock()
	if reply.TaskType == 0{
		if m.mrecord[reply.MapTaskId] == Processing{
			m.mrecord[reply.MapTaskId] = NotStarted
		} 
	}else if reply.TaskType == 1{
		if m.rrecord[reply.ReduceTaskId] == Processing{
			m.rrecord[reply.ReduceTaskId] = NotStarted
		}
	}

}

func (m *Master) FinishedTask(args *FinishedArgs, reply *FinishedReply) error{
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.IsMap{
		m.Mcount++;
		m.mrecord[args.Id] = 2
		
	}else{
		m.Rcount++;
		m.rrecord[args.Id] = 2
	}
	return nil
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
	rpc.Register(m)
	rpc.HandleHTTP()
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
	ret = m.Rcount == m.Nreduce


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
	m.mrecord = make([]int, len(files))
	m.rrecord = make([]int, nReduce)
	m.Nmap = len(files)
	m.Nreduce = nReduce
	m.files = files
	m.Mcount = 0
	m.Rcount = 0

	m.server()
	return &m
}
