package main

//顺序执行的map reduce程序， 用于统计文件中每个单词的个数

import (
	"fmt"
	"log"
	"my6824/mr"
	"os"
	"sort"
)

// ByKey 定义一个切片用于存储排过序的键值对
type ByKey []mr.KeyValue

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

// 入口函数方法
func main() {
	//执行方法 go run mrsequential.go xx.so xx.txt ...
	//入参个数的基本校验
	if len(os.Args) < 3 {
		_, _ = fmt.Fprintln(os.Stderr, "Usage: mrsequential xxx.so input files...")
		os.Exit(1)
	}

	//加载plugin
	mapf, reducef := loadPlugin(os.Args[1])

	//声明一个用于存储所有单词键值对的切片
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.ReadFile(filename)
		if err != nil {
			log.Fatalf("can`t open file %v", filename)
		}

		//调用map函数，返回每个单词的键值对
		kva := mapf(filename, string(file))

		//将结果放到统一的切片上
		intermediate = append(intermediate, kva...)
	}

	//单词排序
	sort.Sort(ByKey(intermediate))

	//创建输出结果的文件
	oname := "mr-out-0"
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("can`t create out file %v %v", oname, err.Error())
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
}

//// 加载plugin, 返回map和reduce的函数
//func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
//	//加载plugin
//	p, err := plugin.Open(filename)
//	if err != nil {
//		log.Fatalf("can`t load plugin %v", filename)
//	}
//
//	//加载Map函数
//	xmapf, err := p.Lookup("Map")
//	if err != nil {
//		log.Fatalf("can`t find Map in %v", filename)
//	}
//
//	//类型推断
//	mapf := xmapf.(func(string, string) []mr.KeyValue)
//
//	//加载Reduce函数
//	xreducef, err := p.Lookup("Reduce")
//	if err != nil {
//		log.Fatalf("can`t find Reduce in %v", filename)
//	}
//
//	reducef := xreducef.(func(string, []string) string)
//
//	return mapf, reducef
//}
