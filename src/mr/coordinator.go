package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "fmt"


type Task struct {
	done				int
	dispatch 		int
	startTime		time.Time
}

type Coordinator struct {
	// Your definitions here.
	files 			[]string
	nReduce 		int
	nMap				int
	mapCount		int
	reduceCount int
	phase 			int
	tasks				[]Task
	mu 					sync.Mutex
	timer 			time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) initMapPhase() {
	c.phase = 1
	c.tasks = make([]Task, c.nMap)
	for i := range c.tasks {
		c.tasks[i].done = 0
		c.tasks[i].dispatch = 0
	}
}

func (c *Coordinator) initReducePhase() {
	c.phase = 2
	c.tasks = make([]Task, c.nReduce)
	for i := range c.tasks {
		c.tasks[i].done = 0
		c.tasks[i].dispatch = 0
	}
}

func (c *Coordinator) MapTaskDone(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ind := args.MapNumber
	if ind < 0 || ind >= c.nMap {
		// ignore the message
		return nil
	}
	if c.tasks[ind].done == 0 {
		c.tasks[ind].done = 1
		c.mapCount++
		if c.mapCount == c.nMap {
			c.initReducePhase()
		}
	}
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ind := args.ReduceNumber
	if ind < 0 || ind >= c.nReduce {
		return nil
	}
	if c.tasks[ind].done == 0 {
		c.tasks[ind].done = 1
		c.reduceCount++
		if c.reduceCount == c.nReduce {
			c.phase = 3
		}
	}
	return nil
}

func (c *Coordinator) AllocateTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timer = time.Now()

	reply.NMap = c.nMap
	reply.NReduce = c.nReduce

	if c.phase == 3 {
		// close the worker
		reply.ReplyType = 3
		return nil
	}

	// map phase or reduce phase
	taskInd := -1
	for i, task := range c.tasks {
		if task.done == 1 {
			continue
		}
		if task.dispatch == 0 {
			// not yet dispatched
			taskInd = i
			break
		} else if time.Since(task.startTime) > 10 * time.Second {
			// dispatched but no response for a long time
			taskInd = i
			break;
		} 
	}
	if taskInd == -1 {
		// no task to be dispatched, let worker sleep
		reply.ReplyType = 0
		return nil
	}

	// set reply message
	reply.ReplyType = c.phase;
	if c.phase == 1 {
		// map phase requires file location
		reply.FilePath = c.files[taskInd]
	}
	reply.Id = taskInd
	// set task status
	c.tasks[taskInd].dispatch = 1
	c.tasks[taskInd].startTime = time.Now()
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
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if time.Since(c.timer) > 10 * time.Second {
		log.Fatalf("No work for a long time. Close the coordinator")
	}
	fmt.Printf("%v\n", c.phase);
	return c.phase == 3
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// initialize the coordinator
	c.files = files
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapCount = 0
	c.reduceCount = 0
	c.initMapPhase()
	c.timer = time.Now()

	c.server()
	return &c
}
// lab1