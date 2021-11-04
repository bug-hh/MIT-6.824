package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MRTask struct {
	MapId    int   // 每个 Id 代表一个处理的文本文件
	ReduceId int   // 同上
	MapTaskCnt   int  // 这个代表 map 任务号
	ReduceTaskCnt int  // 同上，不过是 reduce 任务号
	FileName string // 处理的文件名
	MapNums int  // map 任务总数
	ReduceNums int // reduce 任务总数
	Status   TaskState // 任务有四种状态，1、正在处理 map 任务 2、正在处理 reduce 任务 3、处理闲置等待状态  4、任务完成，退出状态
	MapFunction func(string, string) []KeyValue
	ReduceFunction func(string, []string) string
}

type MRTaskReportArgs struct {
	MapId int
	ReduceId int
	Status TaskState
	IsSuccess bool
	MapTaskCnt int
	ReduceTaskCnt int
	err error
}

type Reply struct {
}

type Args struct {
	Err error
}

type CoordinatorDoneReply struct {
	Done bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
