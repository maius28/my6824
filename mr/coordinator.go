package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type Coordinator struct {
	// Your definitions here.

	//超时时间，超过这个时间还没有接收到worker返回的，认为是失败的，则由master重新分配任务, 默认是10s
	Timeout int

	//todo 要加锁 map task任务管理列表
	MapTasks map[string]Task

	//reduce task任务管理列表
	ReduceTasks map[string]Task
}

// Task 每一个task对应一个split，表示一个map或者reduce的任务
type Task struct {
	//任务编号
	Id string

	//对于map任务来说，就是filename, 对于reduce任务来说，就是分片的key, 这里的key可以认为是task的参数
	Key string

	//状态，0未分配，已分配，已完成
	State int

	//输出结果的文件名称
	Output []string
}

// Your code here -- RPC handlers for the worker to call.

// getTask an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// 任务分配的函数，worker调用这个方法来请求任务。
func (c *Coordinator) getTask(agrs *MrArgs, reply *MrReply) error {
	//这个参数表示是否map阶段已经全部完成
	allMapFinish := true
	//worker申请任务，先申请map任务
	for k, v := range c.MapTasks {
		//如果状态是未分配，则分配这个任务
		if v.State == 0 {
			//分配Map任务给worker执行，告诉worker要执行任务的文件名称、切片的数量和任务类型
			reply.TaskId = v.Id
			reply.Args = k
			reply.TaskType = 0
			reply.Split = len(c.ReduceTasks)
			return nil
		} else if v.State == 1 {
			//如果有任务是在已分配的状态
			allMapFinish = false
		}
	}

	//如果没有申请到map任务，则开始分配reduce任务
	if allMapFinish {
		//如果map阶段已经结束，则给worker分配reduce任务
		for k, v := range c.ReduceTasks {
			if v.State == 0 {
				reply.TaskId = v.Id
				reply.Args = k
				reply.TaskType = 1
				reply.Split = len(c.ReduceTasks)
				return nil
			}
		}
	}

	//其它情况任务类型返回-1，让worker下次再请求任务
	reply.TaskType = -1
	return nil
}

// 通知任务完成
func (c *Coordinator) NotifyTaskFinish(args *MrArgs, reply *MrReply) error {
	task := c.MapTasks[args.TaskId]
	//任务已完成
	task.State = 2

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
	c := Coordinator{
		//10s
		Timeout: 10000,
	}

	//根据参数, 初始化mapTask
	c.MapTasks = make(map[string]Task, len(files))
	for i, filename := range files {
		taskId := "map-" + strconv.Itoa(i)
		c.MapTasks[taskId] = Task{
			Id:     taskId,
			Key:    filename,
			State:  0,
			Output: make([]string, 10),
		}
	}

	//根据参数，初始化reduceTask
	c.ReduceTasks = make(map[string]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		key := strconv.Itoa(i)
		c.ReduceTasks[key] = Task{
			Id:     "reduce-" + key,
			Key:    key,
			State:  0,
			Output: make([]string, 1),
		}
	}

	//your code here

	c.server()
	return &c
}
