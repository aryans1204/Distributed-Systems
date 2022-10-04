package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "bufio"
import "io/ioutil"
import "path/filepath"
import "sort"
import "time"
import "os"


//
// Map functions return a slice of KeyValue.
//
var nReduce int
const TaskInterval = 200

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
	n, succ := getReduceCount()
	if succ == false {
		fmt.Println("Failed to get reduce task count, worker exiting...")
		return
	}
	nReduce = n

	for {
		reply, succ := requestTask()

		if succ == false {
			fmt.Println("Failed to request task, worker exiting...")
			return
		}
		if reply.TaskType == ExitTask {
			fmt.Println("All tasks are done, exiting...")
			return
		}

		exit, succ := false, true

		if reply.TaskType == MapTask {
			doMap(mapf, reply.TaskFile, reply.TaskId)
			exit, succ = reportTaskDone(MapTask, reply.TaskId)
		} else if reply.TaskType == ReduceTask {
			doReduce(reducef, reply.TaskId)
			exit, succ = reportTaskDone(ReduceTask, reply.TaskId)
		}

		if exit || !succ {
			fmt.Println("Coordinator exited or all tasks done, exiting...")
			return
		}
		time.Sleep(time.Millisecond * TaskInterval)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//getReduceCount RPC call
func getReduceCount() (int, bool) {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}

	succ := call("Coordinator.GetReduceCount", &args, &reply)

	return reply.ReduceCount, succ
}

//requestTask RPC call

func requestTask() (*RequestTaskReply, bool) {
	args := RequestTaskArgs{os.Getpid()}
	reply := RequestTaskReply{}

	succ := call("Coordinator.RequestTask", &args, &reply)

	return &reply, succ
}
//reportTaskDone RPC call
func reportTaskDone(taskType TaskType, taskId int) (bool, bool) {
	args := ReportTaskArgs{}
	reply := ReportTaskReply{}

	succ := call("Coordinator.ReportTaskDone", &args, &reply)

	return reply.CanExit, succ
}
//doMap function
func doMap(mapf func(string, string) []KeyValue, filPath string, mapId int) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file, input correct file path...")
		return
	}
	contents, util := ioutil.ReadAll(file)
	file.Close()

	kva := mapf(filePath, string(contents))
	writeMapOutput(kva, mapId)
}
//writeMapOutput function
func writeMapOutput(kva []KeyValue, mapId int) {
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, mapId)
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	// create temp files, use pid to uniquely identify this worker
	for i := 0; i < nReduce; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filePath)
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	// write map outputs to temp files
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err\
		 := encoders[idx].Encode(&kv)
	}

	// flush file buffer to disk
	for _, buf := range buffers {
		err := buf.Flush()
	}

	// atomically rename temp files to ensure no one observes partial files
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		checkError(err, "Cannot rename file %v\n", file.Name())
	}
}
//doReduce function
func doReduce(reducef func(string, []string) string, reduceId int) {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", reduceId))

	kvMap := make(map[string][]string)
	var kv KeyValue

	for _, filepath := range files {
		file, err := os.Open(filepath)

		dec := json.NewEncoder(file)
		for dec.More() {
			err := dec.Decode(&kv)
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}

	}
	writeReduceOutput(reducef, kvMap, reduceId)
}
//writeReduceOutut function
func writeReduceOutput(reducef func(string, []string) string, kvMap map[string][]string, reduceId int) {
	// sort the kv map by key
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create temp file
	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceId, os.Getpid())
	file, err := os.Create(filePath)

	// Call reduce and write to temp file
	for _, k := range keys {
		v := reducef(k, kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, reducef(k, kvMap[k]))
	}

	// atomically rename temp files to ensure no one observes partial files
	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filePath, newPath)
}

//
// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
