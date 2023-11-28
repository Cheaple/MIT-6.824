package mr

import "fmt"
import "hash/fnv"
import "io/ioutil"
import "encoding/json"
import "log"
import "net/rpc"
import "os"
import "sort"
import "strconv"
import "time"


// for sorting by key.
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
	for true {
		reply := callTask()
		// log.Println(reply)
		if reply.TaskType == MapTask {
			// log.Printf("Start map task %d\n", reply.Idx)
			doMap(&reply, mapf)
		} else if reply.TaskType == ReduceTask {
			// log.Printf("Start reduce task %d\n", reply.Idx)
			doReduce(&reply, reducef)
		} else if reply.TaskType == NoTask {
			// Although there are no more task,
			// this worker cannot exit immediately,
			// since there may be some new tasks (due to other worker's failures)
			time.Sleep(2 * time.Second)
		} else {
			break
		}
	}
}


//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		log.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		log.Printf("call failed!\n")
// 	}
// }


//
// Perform Map task
//
func doMap(task *MyReply, mapf func(string, string) []KeyValue) {
	// read the input file, pass it to Map
	filepath := task.FileList[0]
	file, err := os.Open(filepath)
	defer file.Close()
	if err != nil {
		log.Fatalf("Error opening %v", filepath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Error reading %v", filepath)
	}
	
	// partition and store the intermediate 
	kva := mapf(filepath, string(content))
	kvas := partition(kva, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		reduceFilepath := writeMapResults(kvas[i], task.Idx, i)
		_ = notifyMapResults(reduceFilepath, i)
	}
	_ = notifyTaskDone(MsgMapDone, task.Idx)
	// log.Println("Finish map()")
}

//
// Perform Reduce task
//
func doReduce(task *MyReply, reducef func(string, []string) string) {
	// merge data from all input files
	kva := []KeyValue{}
	for _, filepath := range task.FileList {
		file, err := os.Open(filepath)
		defer file.Close()
		if err != nil {
			log.Fatalf("Error opening %v", filepath)
		}
		decoder := json.NewDecoder(file)
		for true {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	// output reduce results
	oname := "mr-out-"+strconv.Itoa(task.Idx)
	ofile, err := ioutil.TempFile(".", "tmp-" + oname)
	if err != nil {
		fmt.Println("Error creating temp file:", err)
	}

	for i := 0; i < len(kva); {
		key := kva[i].Key
		values := []string{}
		for i < len(kva) && key == kva[i].Key  {
			// merge values with the same key together
			values = append(values, kva[i].Value)
			i += 1
		}
		fmt.Fprintf(ofile, "%v %v\n", key, reducef(key, values))
	}
	ofile.Close()
	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Println("Error renaming temp file:", err)
	}
	_ = notifyTaskDone(MsgReduceDone, task.Idx)
	// log.Println("Finish reduce()")
}


// partition intermediate results
func partition(kva []KeyValue, n int) [][]KeyValue {
	kvas := make([][]KeyValue, n)
	for _, kv := range kva {
		i := ihash(kv.Key) % n
		kvas[i] = append(kvas[i], kv)
	}
	return kvas
}

// Write Map output (KeyValue pairs) to a Json file
func writeMapResults(kva []KeyValue, idx int, idxReduce int) string {
	filepath := "mr-" + strconv.Itoa(idx) + "-" + strconv.Itoa(idxReduce)
	file, err := ioutil.TempFile(".", "tmp-" + filepath)
	if err != nil {
		fmt.Println("Error creating temp file:", err)
	}

	encoder := json.NewEncoder(file)
	for _, kv := range kva {
		err := encoder.Encode(&kv)
		if err != nil {
			log.Printf("Error encoding (%v, %v): ", kv.Key, kv.Value)
		}
	}
	file.Close()

	err = os.Rename(file.Name(), filepath)
	if err != nil {
		log.Println("Error renaming temp file:", err)
	}

	return filepath
}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "13.53.175.183"+":1234")
	// c, err := rpc.DialHTTP("tcp", "localhost"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// fails to contact the coordinator
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return false
	}

	return true
}

// Call for a task
func callTask() MyReply {
	args := MyArgs{}
	args.MsgType = MsgTask

	reply := MyReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.CallHandler" tells the
	// receiving server that we'd like to call
	// the CallHandler() method of struct Coordinator.
	ok := call("Coordinator.CallHandler", &args, &reply)
	if !ok {
		return MyReply{TaskType: ExitTask}
	}
	return reply
}

// Send Map result (Json file) path to the master
func notifyMapResults(path string, idxReduce int) MyReply {
	args := MyArgs{}
	args.MsgType = MsgMapOutput
	args.MsgStr = path
	args.MsgInt = idxReduce

	reply := MyReply{}

	ok := call("Coordinator.CallHandler", &args, &reply)
	if !ok {
		log.Println("error when sending Map results to the master")
	}
	return reply
}

// notify finished task
func notifyTaskDone(taskType int, taskIdx int) MyReply {
	args := MyArgs{}
	args.MsgType = taskType
	args.MsgInt = taskIdx
	reply := MyReply{}

	ok := call("Coordinator.CallHandler", &args, &reply)
	if !ok {
		// if the worker fails to contact the coordinator
		// it can assume that the coordinator has exited because the job is done
		return MyReply{TaskType: ExitTask}
	}
	return reply
}