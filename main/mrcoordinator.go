package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"my6824/mr"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Print(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	c := mr.MakeCoordinator(os.Args[1:], 10)
	for c.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
