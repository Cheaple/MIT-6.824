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
		reply := CallTask()
		// fmt.Println(reply)
		if reply.TaskType == MapTask {
			// fmt.Println("Start map()")
			doMap(&reply, mapf)
		} else if reply.TaskType == ReduceTask {
			// fmt.Println("Start reduce()")
			doReduce(&reply, reducef)
		} else if reply.TaskType == FinishTask {
			time.Sleep(time.Second)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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

	fmt.Println(err)
	return false
}


func doMap(task *MyReply, mapf func(string, string) []KeyValue) {
	// read the input file,
	// pass it to Map.
	//
	filepath := task.FileList[0]
	file, err := os.Open(filepath)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filepath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filepath)
	}
	
	// partition and store the intermediate 
	kva := mapf(filepath, string(content))
	kvas := partition(kva, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		reduceFilepath := writeMapResults(kvas[i], task.Idx, i)
		_ = sendMapResults(reduceFilepath, i)
	}
	// fmt.Println("Finish map work")
	_ = callTaskDone(MsgMapDone, task.Idx)
	// fmt.Println("Finish map()")
}

func doReduce(task *MyReply, reducef func(string, []string) string) {
	// merge values with the same key together
	kva := []KeyValue{}
	for _, filepath := range task.FileList {
		file, err := os.Open(filepath)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", filepath)
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
	// fmt.Println(kva)

	oname := "mr-out-"+strconv.Itoa(task.Idx)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	for i := 0; i < len(kva); {
		// fmt.Println(kva[i])
		key := kva[i].Key
		values := []string{}
		for i < len(kva) && key == kva[i].Key  {
			values = append(values, kva[i].Value)
			i += 1
		}
		fmt.Fprintf(ofile, "%v %v\n", key, reducef(key, values))
	}
	_ = callTaskDone(MsgReduceDone, task.Idx)
	// fmt.Println("Finish reduce()")
}

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
	file, _ := os.Create(filepath)

	encoder := json.NewEncoder(file)
	for _, kv := range kva {
		err := encoder.Encode(&kv)
		if err != nil {
			fmt.Printf("cannot encode (%v, %v): ", kv.Key, kv.Value)
		}
	}
	return filepath
}

// Send Map result (Json file) path to the master
func sendMapResults(path string, idxReduce int) MyReply {
	args := MyArgs{}
	args.MsgType = MsgMapOutput
	args.MsgStr = path
	args.MsgInt = idxReduce

	reply := MyReply{}

	ok := call("Coordinator.CallHandler", &args, &reply)
	if !ok {
		fmt.Println("error when sending Map results to the master")
	}
	return reply
}

func CallTask() MyReply {
	args := MyArgs{}
	args.MsgType = MsgTask

	reply := MyReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.CallHandler" tells the
	// receiving server that we'd like to call
	// the CallHandler() method of struct Coordinator.
	ok := call("Coordinator.CallHandler", &args, &reply)
	if !ok {
		return MyReply{TaskType: FinishTask}
	}
	return reply
}

func callTaskDone(taskType int, taskIdx int) MyReply {
	args := MyArgs{}
	args.MsgType = taskType
	args.MsgInt = taskIdx
	reply := MyReply{}

	ok := call("Coordinator.CallHandler", &args, &reply)
	if !ok {
		return MyReply{TaskType: FinishTask}
	}
	return reply
}