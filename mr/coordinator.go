package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	task         []string        // 任务队列
	mapStatus    map[string]bool // map任务状态
	reduceStatus map[string]bool // reduce任务状态
	phase        int             // 当前阶段, map or reduce
	index        int             // worker 标识
	nReduce      int
	mutex        sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RpcHandler(args *RpcArgs, reply *RpcReply) error {
	//fmt.Println(args)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(args.TaskName) != 0 {
		// 提交任务
		if c.phase == 1 {
			c.mapStatus[args.TaskName] = true
			c.attemptChangePhase()
		} else {
			c.reduceStatus[args.TaskName] = true
		}
	}
	// 分配任务
	c.assignTask(reply)
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

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.task) != 0 {
		return false
	}
	if c.phase == 1 {
		return false
	}
	// 检查reduce task是否均完成
	keys := make([]string, 0, len(c.reduceStatus))
	for k := range c.reduceStatus {
		keys = append(keys, k)
	}
	for _, k := range keys {
		if !c.reduceStatus[k] {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.phase = 1
	c.index = 0
	c.nReduce = nReduce
	c.task = make([]string, 0)
	c.mapStatus = make(map[string]bool)
	c.reduceStatus = make(map[string]bool)
	c.task = append(c.task, files...)

	c.server()
	return &c
}

// 分配任务
func (c *Coordinator) assignTask(reply *RpcReply) {
	if len(c.task) == 0 {
		return
	}
	task := c.task[len(c.task)-1]
	c.task = c.task[:len(c.task)-1]
	if len(task) == 0 {
		return
	}
	reply.TaskName = task
	c.index++
	reply.Index = c.index
	reply.Phase = c.phase
	reply.NReduce = c.nReduce
	if c.phase == 1 {
		c.mapStatus[task] = false
	} else {
		c.reduceStatus[task] = false
	}
	// 开启等待
	go c.wait(c.phase, task)

}

// 尝试进入reduce阶段
func (c *Coordinator) attemptChangePhase() {
	if len(c.task) == 0 {
		keys := make([]string, 0, len(c.mapStatus))
		// 遍历mapStatus是否都完成
		for k := range c.mapStatus {
			keys = append(keys, k)
		}
		tag := true
		for _, k := range keys {
			if !c.mapStatus[k] {
				tag = false
				break
			}
		}
		if tag {
			c.phase = 2
			// 将nReduce个reduce任务加入task队列
			for i := 0; i < c.nReduce; i++ {
				c.task = append(c.task, "reduce_"+strconv.Itoa(i+1))
			}
		}
	}
}

// 分配任务后等待
// 等待时间过后，如果没有完成，则认为worker出现故障，应重新加入task队列，分配给下一个worker
func (c *Coordinator) wait(phase int, taskName string) {
	time.Sleep(time.Duration(10) * time.Second)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if (phase == 1 && !c.mapStatus[taskName]) || (phase == 2 && !c.reduceStatus[taskName]) {
		// 任务重新加入队列
		c.task = append(c.task, taskName)
	}
}
