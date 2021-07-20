package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func minInt(x, y int32) int32 {
	if x > y {
		return y
	}
	return x
}
