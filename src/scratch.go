package main

import (
	"fmt"
)

func main() {
	slice := make([]int, 2)
	slice = append(slice[0:0], []int{}...)
	fmt.Printf("%+v\n", slice)
}
