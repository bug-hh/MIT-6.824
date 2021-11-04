package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"plugin"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


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
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	task := MRTask{
		MapFunction: mapf,
		ReduceFunction: reducef,
	}

	for {
		task.getMRTask()
		//fmt.Println("status: ", task.Status)
		if task.Status == MapState {
			task.executeMapTask()
		} else if task.Status == ReduceState {
			task.executeReduceTask()
		} else if task.Status == StopState {
			fmt.Println("StopState")
			break
		} else if task.Status == WaitState {
			time.Sleep(300 * time.Millisecond)
		}
	}
}


func (task *MRTask)reportTask(err error) {
	rpcName := "Coordinator.HandleReport"
	noreply := Reply{}
	newTaskReportArgs := MRTaskReportArgs{
		MapId: task.MapId,
		ReduceId: task.ReduceId,
		MapTaskCnt: task.MapTaskCnt,
		ReduceTaskCnt: task.ReduceTaskCnt,
		Status: task.Status,
		IsSuccess: true,
		err: err,
	}
	if err != nil {
		newTaskReportArgs.IsSuccess = false
	}
	fmt.Printf("执行 reportTask: %v", newTaskReportArgs)
	call(rpcName, &newTaskReportArgs, &noreply)
}

func (task *MRTask)getMRTask() {
	rpcName := "Coordinator.GetTask"
	args := Args{}
	newTask := MRTask{}
	call(rpcName, &args, &newTask)
	//fmt.Println("newTask status", newTask.Status)
	if newTask.Status == MapState {
		task.Status = newTask.Status
		task.MapNums = newTask.MapNums
		task.ReduceNums = newTask.ReduceNums
		task.MapId = newTask.MapId
		task.FileName = newTask.FileName
		task.MapTaskCnt = newTask.MapTaskCnt
	} else if newTask.Status == ReduceState {
		task.Status = newTask.Status
		task.MapNums = newTask.MapNums
		task.ReduceNums = newTask.ReduceNums
		task.ReduceId = newTask.ReduceId
		task.FileName = newTask.FileName
		task.ReduceTaskCnt = newTask.ReduceTaskCnt
	} else if newTask.Status == StopState {
		task.Status = newTask.Status
	} else if newTask.Status == WaitState {
		task.Status = newTask.Status
	}
}

func (task *MRTask)executeMapTask() {
	intermediate := make([][]KeyValue, task.ReduceNums, task.ReduceNums)
	content := readFile(task.FileName)
	kva := task.MapFunction(task.FileName, content)

	for _, kv := range kva {
		index := ihash(kv.Key) % task.ReduceNums
		intermediate[index] = append(intermediate[index], kv)
	}

	for i := 0; i < task.ReduceNums; i++ {
		oname := fmt.Sprintf("mr-%d-%d", task.MapId, i)
		ofile, fileErr := CreateOrOpenFile(oname)
		// 出错后，报告 master
		if fileErr != nil {
			task.reportTask(fileErr)
			fmt.Println("map task: cannot create or open file %v, err: %v", oname, fileErr)
			return
		}
		data, _ := json.Marshal(intermediate[i])
		_, jsonErr := ofile.Write(data)
		// 出错后，报告 master
		if jsonErr != nil {
			task.reportTask(jsonErr)
			fmt.Println("map task: cannot write json to file %v, err: %v", oname, jsonErr)
			return
		}
		ofile.Close()
		fmt.Println("intermediate file: ", oname)
	}
	fmt.Println("execute map task 成功，接着执行 reportTask")
	task.reportTask(nil)
}

func (task *MRTask)executeReduceTask() {
	kvReduce := make(map[string][]string)
	for i := 0; i < task.MapNums; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, task.ReduceId)
		ifile, fileErr := os.Open(iname)
		if fileErr != nil {
			task.reportTask(fileErr)
			fmt.Println("reduce task: cannot create or open file %v, err: %v", iname, fileErr)
			return
		}
		data, dataErr := ioutil.ReadAll(ifile)
		if dataErr != nil {
			task.reportTask(dataErr)
			fmt.Println("reduce task: cannot read file %v, err: %v", iname, dataErr)
			return
		}
		var kvs []KeyValue
		jsonErr := json.Unmarshal(data, &kvs)
		if jsonErr != nil {
			task.reportTask(jsonErr)
			fmt.Println("reduce task: cannot read json from file %v, err: %v", iname, jsonErr)
			return
		}
		for _, kv := range kvs {
			_, ok := kvReduce[kv.Key]
			if !ok {
				kvReduce[kv.Key] = make([]string, 0)
			}
			kvReduce[kv.Key] = append(kvReduce[kv.Key], kv.Value)
		}
		ifile.Close()
	}
	ReduceResult := make([]string, 0)
	for word, timesArr := range kvReduce {
		ReduceResult = append(ReduceResult, fmt.Sprintf("%v %v\n", word, task.ReduceFunction(word, timesArr)))
	}
	oname := fmt.Sprintf("mr-out-%d", task.ReduceId)
	fmt.Println("final output: ", oname)
	writeErr := ioutil.WriteFile(oname, []byte(strings.Join(ReduceResult, "")), 0644)
	if writeErr != nil {
		task.reportTask(writeErr)
		fmt.Println("reduce task: cannot write reduce result to file %v, err: %v", oname, writeErr)
		return
	}
	task.reportTask(nil)

}

func CreateOrOpenFile(fileName string) (*os.File, error){
	var f *os.File
	var err error
	if Exists(fileName) {
		f, err = os.Open(fileName)
	}
	f, err = os.Create(fileName)
	return f, err
}

func Exists(path string) bool {
	_, err := os.Stat(path)    //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func readFile(fileName string) string {
	file, err := os.Open(fmt.Sprintf("../main/%s", fileName))
	if err != nil {
		panic(err)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	return string(content)
}

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
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
	c, err := rpc.DialHTTP("tcp", "127.0.0.1:9090")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)

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
