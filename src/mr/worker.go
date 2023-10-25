package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
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

type ByKey []KeyValue

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

func (reply *TaskReply) CallIdleTask() { //request a task from coordinator
	args := TaskArgs{}
	args.Status = Idle
	call("Coordinator.TaskManager", &args, &reply)
}

func MapWorker(FileName string, MapId int, nReduce int, WorkerId int, mapf func(string, string) []KeyValue) bool { //map task
	//fmt.Println("MapWorker start")
	file, err := os.Open(FileName)
	if err != nil {
		log.Fatalf("cannot open %v", FileName)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", FileName)
		return false
	}
	file.Close()
	intermidiate := mapf(FileName, string(content))

	buffer := make([][]KeyValue, nReduce) //buffer for intermidiate files
	for i := range intermidiate {
		ReduceId := ihash(intermidiate[i].Key) % nReduce
		//write to intermidiate file
		buffer[ReduceId] = append(buffer[ReduceId], intermidiate[i])
	}
	for i := range buffer {
		//write to temp intermidiate file
		tempfile, err := os.Create(fmt.Sprintf("temp-mr-%d-%d-%d", MapId, i, WorkerId))
		if err != nil {
			log.Fatalf("cannot create temp file")
			return false
		}
		defer os.Remove(tempfile.Name())
		encoder := json.NewEncoder(tempfile)
		for _, kv := range buffer[i] {
			err := encoder.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode")
				return false
			}
		}
		err = tempfile.Close()
		if err != nil {
			log.Fatalf("cannot close temp file")
			return false
		}
		//rename temp file to intermidiate file
		err = os.Rename(tempfile.Name(), fmt.Sprintf("mr-%d-%d", MapId, i))
		tempfile.Close()
		if err != nil {
			return false //some other worker has already done this task
		}
	}
	//fmt.Println("MapWorker finished")
	return true
}

func ReduceWorker(ReduceId int, nMap int, WorkerId int, reducef func(string, []string) string) bool {
	Filenames := make([]string, nMap) //intermidiate files names
	for i := 0; i < nMap; i++ {
		Filenames[i] = fmt.Sprintf("mr-%d-%d", i, ReduceId)
	}
	for { //check if all map tasks are finished,if not wait one second
		var flag bool = true
		for i := 0; i < nMap; i++ {
			if _, err := os.Stat(Filenames[i]); os.IsNotExist(err) {
				flag = false
				break
			}
		}
		if flag {
			break //all map tasks are finished
		}
	}
	var intermidiate []KeyValue
	for _, Filename := range Filenames {
		file, err := os.Open(Filename)
		if err != nil {
			log.Fatalf("cannot open %v", Filename)
			return false
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermidiate = append(intermidiate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermidiate))
	// write the output to temp file the rename it after finished
	tempfile, err := os.Create(fmt.Sprintf("temp-mr-out-%d-%d", ReduceId, WorkerId))
	if err != nil {
		log.Fatalf("cannot create temp file")
		return false
	}
	defer os.Remove(tempfile.Name())
	i := 0
	for i < len(intermidiate) {
		j := i + 1
		for j < len(intermidiate) && intermidiate[j].Key == intermidiate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermidiate[k].Value)
		}
		output := reducef(intermidiate[i].Key, values)

		fmt.Fprintf(tempfile, "%v %v\n", intermidiate[i].Key, output)
		i = j
	}
	err = tempfile.Close()
	if err != nil {
		log.Fatalf("cannot close temp file")
		return false
	}
	//rename temp file to intermidiate file
	err = os.Rename(tempfile.Name(), fmt.Sprintf("mr-out-%d", ReduceId))
	tempfile.Close()
	return err == nil
}

func CallFinishTask(TaskId int, TaskType int) { //call the coordinator that the task is finished
	args := TaskArgs{}
	args.Status = Finished
	args.TaskId = TaskId
	args.TaskType = TaskType
	reply := TaskReply{}
	call("Coordinator.TaskManager", &args, &reply)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//fmt.Println("Worker start")
	for {
		reply := TaskReply{}
		reply.CallIdleTask()
		if reply.TaskType == MapTask {
			//fmt.Println("MapTask", reply.TaskId)
			//fmt.Println("Start Map")
			MapWorker(reply.Filename, reply.TaskId, reply.Reduce, reply.WorkerId, mapf)
			CallFinishTask(reply.TaskId, reply.TaskType)
			//fmt.Println("Map Finished")
		} else if reply.TaskType == ReduceTask {
			//fmt.Println("ReduceTask", reply.TaskId)
			//fmt.Println("Start Reduce")
			ReduceWorker(reply.TaskId, reply.Map, reply.WorkerId, reducef)
			CallFinishTask(reply.TaskId, reply.TaskType)
			//fmt.Println("Reduce Finished")
		} else {
			time.Sleep(1 * time.Second) //wait one second for next request
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
