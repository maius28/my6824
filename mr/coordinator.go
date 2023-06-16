package mr

import (
	"encoding/json"
	"fmt"
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

	//超时时间，超过这个时间还没有接收到worker返回的，认为是失败的，则由master重新分配任务, 默认是10s
	Timeout time.Duration

	//map task任务管理列表
	MapTasks map[string]*SafeTask

	//reduce task任务管理列表
	ReduceTasks map[string]*SafeTask
}

type SafeTask struct {
	//加一个读写锁来控制并发
	sync.RWMutex

	Task Task
}

// Your code here -- RPC handlers for the worker to call.

// getTask an example RPC handler.

func (st *SafeTask) changeState(from TaskState, to TaskState) bool {
	//加写锁
	st.Lock()
	defer st.Unlock()

	if st.Task.State == from {
		st.Task.State = to
		return true
	}

	return false
}

// the RPC argument and reply types are defined in rpc.go.
// 任务分配的函数，worker调用这个方法来请求任务。
func (c *Coordinator) GetTask(agrs *MrArgs, reply *Task) error {

	//这个参数表示是否map阶段已经全部完成
	allMapFinish := true
	//worker申请任务，先申请map任务
	for _, st := range c.MapTasks {
		//如果状态是未分配，则分配这个任务
		if st.Task.State == 0 {
			//分配Map任务给worker执行，告诉worker要执行任务的文件名称、切片的数量和任务类型
			success := st.changeState(0, 1)
			if success {
				now := time.Now()
				st.Task.RunAtTime = &now
				*reply = st.Task
				fmt.Printf("apply map task %v to worker\n", reply)
				return nil
			}
		} else if st.Task.State == 1 {
			//如果有任务是在已分配的状态
			allMapFinish = false
		}
	}

	allReduceFinish := true
	//如果没有申请到map任务，则开始分配reduce任务
	if allMapFinish {
		//如果map阶段已经结束，则给worker分配reduce任务
		for _, st := range c.ReduceTasks {
			if st.Task.State == 0 {
				success := st.changeState(0, 1)
				if success {
					now := time.Now()
					st.Task.RunAtTime = &now
					*reply = st.Task
					fmt.Printf("apply reduce task %v to worker\n", st.Task)
					return nil
				}
			} else if st.Task.State == 1 {
				allReduceFinish = false
			}
		}
	}

	//map和reduce全部都跑完了
	if allMapFinish && allReduceFinish {
		fmt.Println("all task finished")
		//worker会根据这个空对象，具体来说是id为空来判断任务全部完成
		reply = &Task{
			TaskType: None,
		}
		return nil
	}

	//其它情况返回-1，让worker下次再请求任务
	fmt.Println("worker should retry")
	reply.TaskType = NeedRetry
	return nil
}

// 通知任务完成
func (c *Coordinator) NotifyTaskFinish(args *MrArgs, reply *string) error {
	fmt.Printf("get task %v finish notify\n", args)
	var st *SafeTask
	if args.TaskType == MapTask {
		st = c.MapTasks[args.TaskId]
	} else {
		st = c.ReduceTasks[args.TaskId]
	}

	if st == nil {
		log.Printf("[ERROR] no id=%v,type=%v task:\n", args.TaskId, args.TaskType)
		return nil
	}

	success := st.changeState(TaskRunning, TaskFinish)
	if success {
		*reply = ""
		return nil
	}

	*reply = fmt.Sprintf("Task Id [%v] state is %v, not support to finish\n", st.Task.Id, st.Task.State)
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
	//your code here
	for _, st := range c.ReduceTasks {
		if st.Task.State != TaskFinish {
			fmt.Println("call done return false")
			return false
		}
	}

	fmt.Println("call done return true")
	return true
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		//10s
		Timeout: 10 * time.Second,
	}

	//根据参数, 初始化mapTask
	c.MapTasks = make(map[string]*SafeTask, len(files))
	for i, filename := range files {
		taskId := "map-" + strconv.Itoa(i)
		task := Task{
			Id:       taskId,
			Key:      filename,
			State:    MapTask,
			TaskType: TaskWaiting,
			Split:    nReduce,
			//Output:   make([]string, 10),
		}
		c.MapTasks[taskId] = &SafeTask{
			Task: task,
		}
	}

	//根据参数，初始化reduceTask
	c.ReduceTasks = make(map[string]*SafeTask, nReduce)
	for i := 0; i < nReduce; i++ {
		key := strconv.Itoa(i)
		task := Task{
			Id:       "reduce-" + key,
			Key:      key,
			State:    TaskWaiting,
			TaskType: ReduceTask,
			Split:    nReduce,
			//Output:   make([]string, 1),
		}
		c.ReduceTasks[task.Id] = &SafeTask{Task: task}
	}

	str, _ := json.MarshalIndent(c, "", "")
	fmt.Printf("Coordinator initial success %s\n", str)

	//这里开启失败重试的协程
	go c.resetTimeoutTask()

	c.server()
	return &c
}

// 重试超时的任务, 如果任务超过10s还没有执行完成，则认为worker已经失联了，直接把任务状态重试为未分配
func (c *Coordinator) resetTimeoutTask() {
	//先不启动，给worker一些时间
	time.Sleep(10 * time.Second)
	fn := func(st *SafeTask, now time.Time) {

		if st.Task.State == TaskRunning {
			if duration := now.Sub(*st.Task.RunAtTime); duration.Seconds() > c.Timeout.Seconds() {
				success := st.changeState(st.Task.State, TaskWaiting)
				if success {
					fmt.Printf("task id %v is timeout, reset to task waiting\n", st.Task)
				}
			}
		}
	}
	for {
		now := time.Now()
		for _, task := range c.MapTasks {
			fn(task, now)
		}

		for _, task := range c.ReduceTasks {
			fn(task, now)
		}

		time.Sleep(time.Second)
	}
}
