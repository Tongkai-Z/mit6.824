package mr

import (
	"errors"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// IDLE map task status
const IDLE = 1

// INPROGRESS map task status
const INPROGRESS = 2

// COMPLETED map task status
const COMPLETED = 3

type Coordinator struct {
	// Your definitions here.
	reduceTaskQueue  chan *Task
	mapTaskQueue     chan *Task
	mapTaskStatus    map[string]*Task
	reduceTaskStatus map[string]*Task
	mapLeft          int
	reduceLeft       int
	done             bool
	mu               sync.Mutex
}

// RequestTask work wait to be assigned a task
// Todo: how could the coordinator manage the uncompleted task due to the worker crash
func (c *Coordinator) RequestTask(req *RequestTaskRequest, res *RequestTaskResponse) error {
	// if there are mapping task left assign one map task to the work, otherwise assign reduce task
	c.mu.Lock()
	ml := c.mapLeft
	rl := c.reduceLeft
	cd := c.done
	c.mu.Unlock()

	if cd {
		return errors.New("Map reduce is finished")
	}
	var taskSend *Task
	if ml > 0 {
		task, ok := <-c.mapTaskQueue
		if !ok {
			res.Task = nil
			return nil
		}
		res.Task = task
		c.mapTaskStatus[task.ID].Status = INPROGRESS
		taskSend = task
	} else if rl > 0 {
		//assign reduce
		task, ok := <-c.reduceTaskQueue
		if !ok {
			return errors.New("all job has been finished")
		}
		res.Task = task
		c.reduceTaskStatus[task.ID].Status = INPROGRESS
		taskSend = task
	} else {
		return errors.New("all job has been finished")
	}
	// Note that defer function is also in the stack, which means it is executed before
	// the invoker of the outer function can get the return value
	// Thus if the defer func below is used, the worker will wait 10 secs to get nil returned
	// Therefore, here I choose to use a new goroutine to wait and monitor the task
	// defer func() {
	// 	// let the server wait for 10 seconds dealing with the potential crash
	// 	for i := 0; i < 10; i++ {
	// 		c.mu.Lock()
	// 		if taskSend.Status == COMPLETED {
	// 			c.mu.Unlock()
	// 			return
	// 		}
	// 		c.mu.Unlock()
	// 		time.Sleep(time.Second)
	// 	}
	// 	// not completed
	// 	if taskSend.IsMap {
	// 		c.mapTaskQueue <- taskSend
	// 	} else {
	// 		c.reduceTaskQueue <- taskSend
	// 	}
	// }()
	go func() {
		// let the server wait for 10 seconds dealing with the potential crash
		for i := 0; i < 10; i++ {
			c.mu.Lock()
			if taskSend.Status == COMPLETED {
				c.mu.Unlock()
				return
			}
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
		// not completed
		c.mu.Lock()
		if taskSend.IsMap && c.mapLeft > 0 {
			c.mu.Unlock()
			// check if the chan is closed or not
			c.mapTaskQueue <- taskSend
		} else if c.reduceLeft > 0 {
			c.mu.Unlock()
			c.reduceTaskQueue <- taskSend
		}
	}()
	return nil
}

// FinishMap map task output mr-X-Y, x is task id, Y is the hash value
func (c *Coordinator) FinishMap(req *FinishMapRequest, res *FinishMapResponse) error {
	task := req.Task
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.mapTaskStatus[task.ID]; ok {
		c.mapTaskStatus[task.ID].Status = COMPLETED
		c.mapLeft--
		// add the intermediateFiles to corresponding reduce task
		for filename, hashvalue := range req.IntermediateFiles {
			if reduceTask, ok := c.reduceTaskStatus[hashvalue]; ok {
				reduceTask.Files = append(reduceTask.Files, filename)
			} else {
				reduceTask := Task{}
				reduceTask.IsMap = false
				reduceTask.Files = []string{}
				reduceTask.Files = append(reduceTask.Files, filename)
				reduceTask.Status = IDLE
				reduceTask.ID = hashvalue
				c.reduceTaskStatus[hashvalue] = &reduceTask
				c.reduceLeft++
			}
		}
	}
	// get the map result file and put them in corresponding reduce task
	if c.mapLeft == 0 {
		close(c.mapTaskQueue)
		// add all reduce task to reduce queue
		for _, task := range c.reduceTaskStatus {
			c.reduceTaskQueue <- task
		}
	}
	return nil
}

// FinishReduce notify master that reduce is finished
func (c *Coordinator) FinishReduce(req *FinishReduceRequest, res *FinishReduceResponse) error {
	c.mu.Lock()
	c.reduceTaskStatus[req.Task.ID].Status = COMPLETED
	c.reduceLeft--
	if c.reduceLeft == 0 {
		close(c.reduceTaskQueue)
		c.done = true
	}
	c.mu.Unlock()
	return nil
}

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
	rpc.Register(c)  // publish the receiver's method in ther server
	rpc.HandleHTTP() //register http handler in the server
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
	ret := false
	c.mu.Lock()
	if c.done {
		ret = true
	}
	c.mu.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.initializeMapTask(files, nReduce)
	c.reduceTaskQueue = make(chan *Task, nReduce)
	c.reduceTaskStatus = make(map[string]*Task)
	c.done = false
	c.server()
	return &c
}

//
// we have nReduce map tasks, thus each task should have ceil(len(files)/nReduce) files
//
func (c *Coordinator) initializeMapTask(files []string, nReduce int) {
	c.mapTaskQueue = make(chan *Task, nReduce)
	c.mapTaskStatus = make(map[string]*Task)
	id := 0
	interval := int(math.Ceil(float64(len(files)) / float64(nReduce)))
	i := 0
	for i < len(files) {
		task := Task{}
		task.Files = files[i:min(i+interval, len(files))]
		task.IsMap = true
		task.ID = strconv.Itoa(id)
		task.Status = IDLE
		task.NReduce = nReduce
		c.mapTaskQueue <- &task
		c.mapTaskStatus[task.ID] = &task
		i += interval
		id++
	}
	c.mapLeft = len(c.mapTaskQueue)
}

func min(x int, y int) int {
	if x > y {
		return y
	}
	return x
}
