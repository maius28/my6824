package main

import (
	"my6824/mr"
	"strconv"
	"strings"
	"unicode"
)

//一个统计单词个数的map reduce application

//go build -buildmode=plugin wc.go

// map函数，每次执行遍历一个文件。第一个参数是文件名，第二个参数是文件内容，暂时可以忽略第一个参数，返回值是键值对的切片
func Map(filename string, content string) []mr.KeyValue {
	//ff函数判断是否是分隔符
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	//将文本进行解析，按分隔符返回所有单词的切片
	words := strings.FieldsFunc(content, ff)

	//声明返回结果
	kva := []mr.KeyValue{}
	//遍历所有单词，生成key value的键值对切片
	for _, word := range words {
		kva = append(kva, mr.KeyValue{word, "1"})
	}

	return kva
}

// reduce函数,接收一个key的map task产生的数据，并做处理
func Reduce(key string, values []string) string {
	//返回数组的长度，即单词出现的次数
	return strconv.Itoa(len(values))
}
