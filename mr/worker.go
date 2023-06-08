package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

// 定义一个key value的结构体，用于保存键值对
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32()
	h.Write([]byte(key))
	//这里将h.Sum32()返回的32无符号整数最高位置为0，将结果转换为有符号的整数
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	//1.申请任务，接收coordinator的分配，然后执行
	askTask()
}

// AskTask 申请任务，表示可以接受map或者reduce任务的执行
func askTask() (reply MrReply) {
	args := MrArgs{}

	reply = MrReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("reply %v\n", reply)
	} else {
		fmt.Printf("call failed\n")
	}

	return
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	args := ExampleArgs{}

	args.X = 99

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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, agrs interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := CoordinatorSock()
	client, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("listening err", err)
	}

	defer client.Close()

	err = client.Call(rpcname, agrs, reply)
	if err != nil {
		fmt.Println(err)
		return false
	}

	return true
}
