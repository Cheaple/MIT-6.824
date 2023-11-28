package mr

// import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

const (
	NotStart = iota
	Started
	Finished
)

type Coordinator struct {
	// Your definitions here.
	nReduce int
	mapFiles []string
	reduceFiles [][]string
	mapStatus []int
	reduceStatus []int
	ifReduceDone bool

	mapTaskChan chan int
	reduceTaskChan chan int
	mapStartChan chan int
	mapTimeoutChan chan int
	mapDoneChan chan int
	reduceStartChan chan int
	reduceTimeoutChan chan int
	reduceDoneChan chan int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) CallHandler(args *MyArgs, reply *MyReply) error {
	// log.Println("Receive message: ", args)
	switch args.MsgType {
	case MsgTask:
		select {
		case i := <- c.mapTaskChan:
			reply.TaskType = MapTask
			reply.Idx = i
			reply.NReduce = c.nReduce
			reply.FileList = []string{c.mapFiles[i]}
			c.mapStartChan <- i
		case i := <- c.reduceTaskChan:
			reply.TaskType = ReduceTask
			reply.Idx = i
			reply.FileList = c.reduceFiles[i]
			c.reduceStartChan <- i
		default:
			reply.TaskType = NoTask
		}
	case MsgMapOutput:
		idx := args.MsgInt
		filepath := args.MsgStr
		c.mu.Lock()
		c.reduceFiles[idx] = append(c.reduceFiles[idx], filepath)
		c.mu.Unlock()
	case MsgMapDone:
		c.mapDoneChan <- args.MsgInt
		reply.Idx = args.MsgInt
	case MsgReduceDone:
		c.reduceDoneChan <- args.MsgInt
		reply.Idx = args.MsgInt
	}
	
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", "0.0.0.0" + ":1234")  // distributed version for bonus
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)  // local version
	if e != nil {
		log.Fatal("Error listening:", e)
	}
	go http.Serve(l, nil)
	go c.manageTasks()
}

//
// Manage tasks (all operations on task status are transported to (via channels) and handled in this function)
//
func (c *Coordinator) manageTasks() {
	// Allocate map tasks to clients
	for i, status := range c.mapStatus {
		if status == NotStart {
			c.mapTaskChan <- i
		}
	}

	// Wait until all map tasks get done
	wait := true
	for wait {
		select {
		case i := <- c.mapStartChan:
			// log.Printf("map task %v start\n", i)
			if c.mapStatus[i] == NotStart {
				// log.Printf("Timer map task %v\n", i)
				go c.taskTimer(MapTask, i)
				c.mapStatus[i] = Started
			}
		case i := <- c.mapDoneChan:
			// log.Printf("map task %v done\n", i)
			c.mapStatus[i] = Finished
		case i := <- c.mapTimeoutChan:
			if c.mapStatus[i] == Started {
				// log.Printf("map task %v timeout\n", i)
				c.mapTaskChan <- i
				c.mapStatus[i] = NotStart
			}

		default:
		}	
		func() {
			wait = false
			for _, status := range c.mapStatus {
				if status != Finished {
					wait = true
					break
				}
			}
		}()
		// time.Sleep(time.Second)
	}

	// Allocate reduce tasks
	for i, status := range c.reduceStatus {
		if status == NotStart {
			c.reduceTaskChan <- i
		}
	}

	// Wait until all reduce tasks get done
	wait = true
	for wait {
		select {
		case i := <- c.reduceStartChan:
			if c.reduceStatus[i] == NotStart {
				// log.Printf("Timer reduce task %v\n", i)
				go c.taskTimer(ReduceTask, i)
				c.reduceStatus[i] = Started
			}
		case i := <- c.reduceDoneChan:
			c.reduceStatus[i] = Finished
		case i := <- c.reduceTimeoutChan:
			if c.reduceStatus[i] == Started {
				// log.Printf("reduce task %v timeout\n", i)
				c.reduceTaskChan <- i
				c.reduceStatus[i] = NotStart
			}
		default:
		}	
		func() {
			wait = false
			for _, status := range c.reduceStatus {
				if status != Finished {
					wait = true
					break
				}
			}
		}()
		// time.Sleep(time.Second)
	}
	c.ifReduceDone = true
}

//
// Monitor task timeout
//
func (c *Coordinator) taskTimer(taskType int, idx int) {
	ticker := time.NewTicker(15 * time.Second)  // some tasks are really slow, so the timer should not be too short
	defer ticker.Stop()
	for true {
		select {
		case <- ticker.C:
			if taskType == MapTask {
				c.mapTimeoutChan <- idx
			} else if taskType == ReduceTask {
				c.reduceTimeoutChan <- idx
			}
			// log.Printf("timeout %v-%v\n", taskType, idx)
			return
		default:
		}
		// time.Sleep(time.Second)
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.ifReduceDone
	if ret == true {
		time.Sleep(time.Second * 10)  // in case that some workers are still calling master
	}
	
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTaskChan = make(chan int, 10)
	c.reduceTaskChan = make(chan int, nReduce)
	c.mapStartChan = make(chan int, 1)
	c.mapDoneChan = make(chan int, 1)
	c.mapTimeoutChan = make(chan int, 1)
	c.reduceStartChan = make(chan int, 1)
	c.reduceTimeoutChan = make(chan int, 1)
	c.reduceDoneChan = make(chan int, 1)

	c.mapStatus = make([]int, len(files))
	c.reduceStatus = make([]int, nReduce)
	c.nReduce = nReduce
	c.mapFiles = files
	c.reduceFiles = make([][]string, nReduce)
	c.ifReduceDone = false

	c.server()
	return &c
}