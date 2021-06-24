package raft

import (
	"fmt"
	"sync/atomic"
)

func main() {
	var x int32
	x = 0
	for i := 0; i < 10; i++ {
		go func() {
			atomic.AddInt32(&x, 1)
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			// atomic.LoadInt32(&x)
			fmt.Printf("reader%d:%d ", i, x)
		}()
	}
}
