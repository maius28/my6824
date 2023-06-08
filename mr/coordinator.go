package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.

	//超时时间，超过这个时间还没有接收到worker返回的，认为是失败的，则由master重新分配任务, 默认是10s
	Timeout int

	//文件名称
	Files []string
}

// Task 每一个task对应一个split，表示一个map或者reduce的任务
type Task struct {
	//文件名称
	Filename string

	//是否完成
	Done bool
}

// Your code here -- RPC handlers for the worker to call.

// AssignTask an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// 任务分配的函数，worker调用这个方法来请求任务。
func (c *Coordinator) AssignTask(agrs *MrArgs, reply *MrReply) error {
	//

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	sockname := CoordinatorSock()
	os.Remove(sockname)

	listen, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatalf("listen err %v", err)
	}

	go http.Serve(listen, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	//your code here

	return ret
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	//your code here

	c.server()
	return &c
}
