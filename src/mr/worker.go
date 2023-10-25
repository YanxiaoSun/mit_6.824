package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "os"
import "io/ioutil"
import "strconv"
import "sort"
import "time"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }



//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for{
		args := WorkerArgs{}
		reply := WorkerReply{}
		call("Master.RequestTask", &args, &reply)
		if reply.TaskType == 0{
			log.Printf("received map task %s", reply.Filename)
			err := HandleMap(reply, mapf)
			if err != nil{
				log.Fatal(err)
				return
			}
		}else if reply.TaskType == 1{
			log.Printf("received reduce task %d", reply.ReduceTaskId)
			err := HandleReduce(reply, reducef)
			if err != nil{
				log.Fatal(err)
				return
			}
		}else{
			time.Sleep(time.Second)
		}

	}

}
func HandleMap(reply WorkerReply, mapf func(string, string) []KeyValue) error{
	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil{
		log.Fatalf("can't open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil{
		log.Fatalf("can't read %v", filename)
	}
	intermediate := []KeyValue{}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	files := make([]*os.File, reply.Nreduce)
	for i := 0; i < reply.Nreduce; i++{
		oname :=  "mr-" + strconv.Itoa(reply.MapTaskId) + "-" + strconv.Itoa(i)
		file, _ := os.Create(oname)
		files[i] = file
	}
	for _, kva := range intermediate{
		index := ihash(kva.Key)%reply.Nreduce
		enc := json.NewEncoder(files[index])
		enc.Encode(&kva)
	}
	call("Master.FinishedTask", &FinishedArgs{true, reply.MapTaskId}, &FinishedReply{})
	return nil
}

func HandleReduce(reply WorkerReply, reducef func(string, []string) string) error{
	intermediate := []KeyValue{}
	for i:=0; i < reply.Nmap; i++{
		oname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskId)
		file,err := os.Open(oname)
		if err != nil{
			log.Fatalf("cant't open %v", oname)
		}
		dec := json.NewDecoder(file)
		defer file.Close()
		for{
			var kva KeyValue
			if err := dec.Decode(&kva); err != nil{
				break
			}
			intermediate  = append(intermediate, kva)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reply.ReduceTaskId)
	ofile, err := ioutil.TempFile("", oname + "*")
	if err != nil{
		log.Fatalf("can't create file %v", oname)
	}
	
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	err = os.Rename(ofile.Name(), oname)
	if err != nil{
		log.Fatalf(err.Error())
	}
	call("Master.FinishedTask", &FinishedArgs{false, reply.ReduceTaskId}, &FinishedReply{})
	return nil		
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
