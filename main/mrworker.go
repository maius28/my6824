package main

import (
	"fmt"
	"log"
	"my6824/mr"
	"os"
	"plugin"
)

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	mr.Worker(mapf, reducef)
}

// 加载plugin, 返回map和reduce的函数
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	//加载plugin
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("can`t load plugin %v", filename)
	}

	//加载Map函数
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("can`t find Map in %v", filename)
	}

	//类型推断
	mapf := xmapf.(func(string, string) []mr.KeyValue)

	//加载Reduce函数
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("can`t find Reduce in %v", filename)
	}

	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
