package mr

import (
	"os"
	"strconv"
)

//RPC定义

// MrArgs worker请求任务的参数
type MrArgs struct {
	TaskId string
}

// MrReply coordinator的返回值
type MrReply struct {
	//任务编号
	TaskId string
	//任务参数
	Args string
	//任务类型，0是MAP任务，1是reduce任务,-1没有任务
	TaskType int
	//切片数量
	Split int
}

// 下面是示例的输入和输出参数
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
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
