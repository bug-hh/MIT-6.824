package mr

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)
import "net/rpc"
import "net/http"

type TaskState int

const (
	MapState TaskState = 0
	ReduceState TaskState = 1
	StopState TaskState = 2
	WaitState TaskState = 3
)

type Flag struct {
	processing bool
	finished bool
}


type Coordinator struct {
	// Your definitions here.
	FileNames []string
	MapNums int  // Map 任务数
	ReduceNums int  // Reduce 任务数
	MapFlags []Flag  // 一个文本文件对应一个数组下标，数组元素代表对这个文本文件执行 map 操作的状态
	ReduceFlags []Flag  // 同上，只是换成 reduce 操作
	MapTaskCnts []int   // 一个文本文件对应一个数组下标，数组元素代表对这个文本文件执行 map 操作的任务号，任务号是递增的，因为有可能失败，导致重试，所以任务号++
	ReduceTaskCnts []int  // 同上，只不过是 reduce 任务
	MapAllDone bool
	ReduceAllDone bool
	Mut sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

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
	err := rpc.Register(c)
	if err != nil {
		fmt.Printf("rpc server register faild, err:%s", err)
	}
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

func (c *Coordinator)httpServer() {
	err := rpc.Register(c)
	if err != nil {
		fmt.Printf("rpc server register faild, err:%s", err)
	}
	rpc.HandleHTTP()
	fmt.Printf("server start ....")
	go http.ListenAndServe(":9090", nil)
}

func (c *Coordinator) HandleTimeout() {
	for {
		c.Mut.Lock()
		if c.MapAllDone && c.ReduceAllDone {
			c.Mut.Unlock()
			break
		}
		time.Sleep(30 * time.Millisecond)
		if !c.MapAllDone {
			for i := 0; i < c.MapNums; i++ {
				if !c.MapFlags[i].finished {
					c.MapFlags[i].processing = false
				}
			}
		} else {
			for i := 0; i < c.ReduceNums; i++ {
				if !c.ReduceFlags[i].finished {
					c.ReduceFlags[i].processing = false
				}
			}
		}
		c.Mut.Unlock()
		time.Sleep(2000 * time.Millisecond)
	}
}

func (c *Coordinator) HandleReport(report *MRTaskReportArgs, noreply *Reply) error {
	c.Mut.Lock()
	defer c.Mut.Unlock()
	fmt.Printf("执行 HandleReport: %v\n", *report)
	if report.IsSuccess {
		if report.Status == MapState {
			// 检查 report 是否与当前 task 匹配
			fmt.Println("HandleReport MapState-- Cnt: \n", report.MapTaskCnt == c.MapTaskCnts[report.MapId])
			if report.MapTaskCnt == c.MapTaskCnts[report.MapId] {
				c.MapFlags[report.MapId].finished = true
				c.MapFlags[report.MapId].processing = false
			}
		} else {
			// 检查 report 是否与当前 task 匹配
			if report.ReduceTaskCnt == c.ReduceTaskCnts[report.ReduceId] {
				c.ReduceFlags[report.ReduceId].finished = true
				c.ReduceFlags[report.ReduceId].processing = false
			}
		}
	} else {
		if report.Status == MapState {
			// 检查 report 是否与当前 task 匹配
			if !c.MapFlags[report.MapId].finished {
				c.MapFlags[report.MapId].processing = false
			}
		} else {
			// 检查 report 是否与当前 task 匹配
			if !c.ReduceFlags[report.ReduceId].finished {
				c.ReduceFlags[report.ReduceId].processing = false
			}
		}

	}

	for i := 0; i < c.MapNums; i++ {
		if !c.MapFlags[i].finished {
			break
		} else if i == c.MapNums - 1 {
			c.MapAllDone = true
		}

	}

	for i := 0; i < c.ReduceNums; i++ {
		if !c.ReduceFlags[i].finished {
			break
		} else if i == c.ReduceNums - 1 {
			c.ReduceAllDone = true
		}
	}
	return nil
}

func (c *Coordinator) GetTask(args *Args, workTask *MRTask) error {
	c.Mut.Lock()
	defer c.Mut.Unlock()
	//fmt.Println("get task invoke")
	fmt.Println("c.MapAllDone: ", c.MapAllDone)

	if !c.MapAllDone {
		//fmt.Println("c.MapAllDone: ", c.MapAllDone)
		for i := 0; i < c.MapNums; i++ {
			//fmt.Printf("c.MapFlags[%d].finished: ", i, c.MapFlags[i].finished)
			//fmt.Printf("c.MapFlags[%d].processing: ", i, c.MapFlags[i].processing)
			if !c.MapFlags[i].finished && !c.MapFlags[i].processing {
				workTask.MapId = i
				c.MapTaskCnts[i]++
				workTask.MapTaskCnt = c.MapTaskCnts[i]
				workTask.MapNums = c.MapNums
				workTask.ReduceNums = c.ReduceNums
				workTask.Status = MapState
				workTask.FileName = c.FileNames[i]
				c.MapFlags[i].processing = true
				fmt.Println("get task invoke   status: ", workTask.Status)
				return nil
			}
		}
		workTask.Status = WaitState
		return nil
	}
	fmt.Println("c.ReduceAllDone", c.ReduceAllDone)
	if !c.ReduceAllDone {
		for i := 0; i < c.ReduceNums; i++ {
			if !c.ReduceFlags[i].finished && !c.ReduceFlags[i].processing {
				workTask.ReduceId = i
				c.ReduceTaskCnts[i]++
				workTask.ReduceTaskCnt = c.ReduceTaskCnts[i]
				workTask.MapNums = c.MapNums
				workTask.ReduceNums = c.ReduceNums
				workTask.Status = ReduceState
				c.ReduceFlags[i].processing = true
				return nil
			}
		}
		workTask.Status = WaitState
		return nil
	}
	workTask.Status = StopState
	return nil
}
//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.MapAllDone && c.ReduceAllDone
}

func generateTaskId() int {
	rand.Seed(time.Now().Unix())
	return rand.Int()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		FileNames: files,
		MapNums: len(files),
		ReduceNums: nReduce,
		MapFlags: make([]Flag, len(files)),
		ReduceFlags: make([]Flag, nReduce),
		MapTaskCnts: make([]int, len(files)),
		ReduceTaskCnts: make([]int, nReduce),
		MapAllDone: false,
		ReduceAllDone: false,
	}
	// Your code here.
	go c.HandleTimeout()
	c.httpServer()
	return &c
}
