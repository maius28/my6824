package mr

import (
	"os"
	"strconv"
	"time"
)

// RPC定义
type TaskState int
type TaskType int

const (
	TaskWaiting = iota
	TaskRunning
	TaskFinish
)

const (
	MapTask = iota
	ReduceTask
	None
	NeedRetry
)

// Task 每一个task对应一个split，表示一个map或者reduce的任务
type Task struct {
	//任务编号
	Id string

	//对于map任务来说，就是filename, 对于reduce任务来说，就是分片的key
	Key string

	//任务类型，0是MAP任务，1是reduce任务, -1表示需要等待任务分配，下次再来请求
	TaskType TaskType

	//切片数量
	Split int

	//状态，0未分配，已分配，已完成
	State TaskState

	//任务开始时间，用于判断超时
	RunAtTime *time.Time
}

// MrArgs worker请求任务的参数
type MrArgs struct {
	TaskId   string
	TaskType TaskType
}

//在这里完成你的rpc代码

// CoordinatorSock Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func CoordinatorSock() string {
	s := "/var/tmp/5840-mr"
	//获取用户进程ID
	s += strconv.Itoa(os.Getuid())
	return s
}
