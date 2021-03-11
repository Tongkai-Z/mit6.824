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
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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
// mapper will map the strings from the files into n subfiles
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	done := true
	for done {
		done = processTask(mapf, reducef)
		// prevent busy wait
		time.Sleep(100 * time.Millisecond)
	}
}

// ProcessTask use rpc to communicate to coordinator to get a map or reduce task
// send the finished msg back
func processTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	req := RequestTaskRequest{}
	res := RequestTaskResponse{}
	if !call("Coordinator.RequestTask", &req, &res) { //no task remaining
		return false
	}
	finishTask(res.Task, mapf, reducef)
	return true
}

// It is possible that the work receive a task which is completed due to the crash recovery
func finishTask(task *Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	if task != nil && task.Status != COMPLETED {
		if task.IsMap {
			mapTask(task, mapf)
		} else {
			reduceTask(task, reducef)
		}
	}
}

// mapTask map the input files to intermediateFiles
// naming: mr-X-Y, X map task ID, Y reduce task ID
func mapTask(task *Task, mapf func(string, string) []KeyValue) {
	finishMap := FinishMapRequest{}
	res := FinishMapResponse{}
	finishMap.Task = task
	finishMap.IntermediateFiles = make(map[string]string)
	fileMap := make(map[string]*os.File)
	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content)) //slice of mr.KeyValue{string, string}
		storeFile(task, kva, &fileMap, &finishMap.IntermediateFiles)
	}
	//close all files
	for _, file := range fileMap {
		file.Close()
	}
	// notify the coordinator that the mapping task is completed
	call("Coordinator.FinishMap", &finishMap, &res)
}

// reduceTask stores final output in mr-out-hash
func reduceTask(task *Task, reducef func(string, []string) string) {
	// read the files into mem and sort them based on key
	intermediate := []KeyValue{}
	for _, fileName := range task.Files {
		file, _ := os.Open(fileName)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	//sort the kva and extract the key arrays
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + task.ID
	//fmt.Printf("reduce output %v\n", oname)
	ofile, _ := os.Create(oname)
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

	ofile.Close()
	freq := FinishReduceRequest{}
	freq.Task = task
	freq.Files = []string{}
	freq.Files = append(freq.Files, oname)
	fres := FinishReduceResponse{}
	call("Coordinator.FinishReduce", &freq, &fres)
}

func storeFile(task *Task, kva []KeyValue, fileMap *map[string]*os.File, resultMap *map[string]string) {
	//file name mr-mapID-termHash
	for _, kv := range kva {
		hash := strconv.Itoa(ihash(kv.Key) % task.NReduce)
		fileName := "mr-" + task.ID + "-" + hash
		file, ok := (*fileMap)[fileName]
		if !ok { // create the file first
			file, _ = os.Create(fileName)
			(*fileMap)[fileName] = file
			(*resultMap)[fileName] = hash
		}
		enc := json.NewEncoder(file)
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Encode error: %v", err)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

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

	log.Println(err)
	return false
}
