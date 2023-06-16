package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/fs"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

// 定义一个key value的结构体，用于保存键值对
type KeyValue struct {
	Key   string
	Value string
}

// ByKey 定义一个切片用于存储排过序的键值对
type ByKey []KeyValue

// Len 实现sort的接口
func (b ByKey) Len() int {
	return len(b)
}

func (b ByKey) Less(i, j int) bool {
	return b[i].Key < b[j].Key
}

func (b ByKey) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
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
	// Your worker implementation here

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	//1.申请任务，接收coordinator的分配，然后执行
	for {
		fmt.Println("start ask task, PID:", os.Getpid())
		task, err := askTask()
		if err != nil {
			//rpc调用失败，则认为连接不上coordinator，直接退出。
			fmt.Println(err.Error())
			os.Exit(1)
			return
		}

		//根据task的具体信息来执行任务
		//map
		switch task.TaskType {
		case None:
			//0时，肯定是返回了Task{}空对象，此时表示所有任务已经处理完成了。没有任务可以分配了。
			fmt.Println("all task finished, exit")
			os.Exit(1)
		case MapTask:
			//map任务
			log.Printf("get map task id[%v]\n", task.Id)
			err := handleMapTask(task, mapf)
			if err == nil {
				log.Printf("notify map task id[%v] finished\n", task.Id)
				notifyTaskFinish(task)
			}
		case ReduceTask:
			//reduce任务
			log.Printf("get reduce task id[%v]\n", task.Id)
			err := handleReduceTask(task, reducef)
			if err == nil {
				log.Printf("notify reduce task id[%v] finished\n", task.Id)
				notifyTaskFinish(task)
			}
		case NeedRetry:
			//当前任务全部都在执行中，暂时无法分配，这种情况需要再次调用
			fmt.Println("no task need to assign, will retry after 2 second...")
		default:
			//未知错误
			fmt.Println("unknown taskType value :", task.TaskType)
			os.Exit(1)
		}

		time.Sleep(2 * time.Second)
	}
}

func notifyTaskFinish(task Task) {
	args := MrArgs{
		TaskId:   task.Id,
		TaskType: task.TaskType,
	}

	var reply *string

	ok := call("Coordinator.NotifyTaskFinish", &args, &reply)
	if ok {
		if len(*reply) == 0 {
			//通知成功
			fmt.Printf("reply %v\n, notified task finish", *reply)
		} else {
			//调用成功，但是可能server端的任务状态并不是处理中，这种情况下，本次reduce任务的结果就不会被采用。
			fmt.Printf("coordinator reply [%v], discard", *reply)
		}

	} else {
		//如果调用失败,返回error
		fmt.Printf("call notifyTaskFinish failed, rpc error\n")
	}
}

// 处理reduce任务, reduce需要读取所有相应分区的文件，排序并统一输出到mr-out-x文件中
func handleReduceTask(task Task, reducef func(string, []string) string) error {
	intermediateFilenames := []string{}

	//找到所有mr-*-index的文件
	err := filepath.Walk(".", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		reg, err := regexp.Compile(`^mr-map-.+-` + task.Key + "$")
		if err != nil {
			return err
		}

		if !info.IsDir() && reg.MatchString(info.Name()) {
			intermediateFilenames = append(intermediateFilenames, path)
		}

		return nil
	})

	if err != nil {
		return err
	}

	log.Printf("reduce find intermediate files %v\n", intermediateFilenames)

	if len(intermediateFilenames) == 0 {
		//没有找到中间文件，理论上来说是有问题的
		log.Println("[WARN]can`t find any intermediateFiles, so  reduce output is empty!")
		return nil
	}

	//读取所有mr-x-index的文件内容
	var intermediate []KeyValue
	for _, filename := range intermediateFilenames {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		//按行读取
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var kv KeyValue
			err := json.Unmarshal(scanner.Bytes(), &kv)
			if err != nil {
				log.Println("[ERROR]" + err.Error())
				break
			}

			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	//排序
	sort.Sort(ByKey(intermediate))

	//log.Printf("sorted %v", intermediate)

	//创建输出结果的文件
	oname := "mr-out-" + task.Key
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}

	//遍历排好序的intermediate，将同一个单词的键值对找出来，并调用reduce函数统计字符个数
	i := 0
	for i < len(intermediate) {
		//用双游标找出相同单词的索引起止点
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}

		words := []string{}
		for k := i; k < j; k++ {
			words = append(words, intermediate[k].Key)
		}

		//统计个数
		count := reducef(intermediate[i].Key, words)

		//写入输出文件
		_, _ = fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, count)

		//遍历下一个单词
		i = j
	}

	_ = ofile.Close()

	return nil
}

// AskTask 申请任务，表示可以接受map或者reduce任务的执行
// 如果error不为nil,则worker被正常分配到了任务
func askTask() (Task, error) {
	args := MrArgs{}

	var reply Task

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("reply %v, start to process task\n", reply)
		return reply, nil
	} else {
		//如果调用失败,返回error
		return Task{}, fmt.Errorf("call failed, rpc error\n")
	}
}

// 写入或者追加文件内容
func writeFile(filename string, content string) error {
	//如果文件存在，则会truncate，lab1中不会出现多个进程同时写入一个文件的情况
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("can`t create intermediate file %v %v\n", filename, err.Error())
	}

	//写入内容
	_, err = fmt.Fprint(file, content)
	if err != nil {
		log.Fatalf("write file error %v %v\n", file, err.Error())
	}

	return nil
}

// map任务处理
func handleMapTask(task Task, mapf func(string, string) []KeyValue) error {
	filename := task.Key
	file, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	//调用map函数，返回每个单词的键值对
	kva := mapf(filename, string(file))

	//把key-value写入到intermediate, todo 这里可以先分组，再创建文件
	partition := make(map[int][]KeyValue, task.Split)
	for _, kv := range kva {
		//分片
		index := ihash(kv.Key) % task.Split

		partition[index] = append(partition[index], kv)
	}

	//把中间结果写入到文件中，用json格式去序列化
	for k, kvs := range partition {
		if len(kvs) == 0 {
			continue
		}

		outputFile := fmt.Sprintf("mr-%v-%v", task.Id, k)
		file, err := os.Create(outputFile)
		if err != nil {
			return err
		}

		encoder := json.NewEncoder(file)
		for _, kv := range kvs {
			err := encoder.Encode(&kv)
			if err != nil {
				return err
			}
		}

		file.Close()
	}

	return nil
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
