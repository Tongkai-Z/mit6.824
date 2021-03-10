package main

import (
	"fmt"
	"hash/fnv"
	"time"
)

func main() {
	fmt.Printf("%v", wait())
}
func wait() int {
	defer func() {
		fmt.Printf("I am waiting for 4 secs\n")
		time.Sleep(4 * time.Second)
		fmt.Printf("time is up\n")
	}()
	return 1
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
