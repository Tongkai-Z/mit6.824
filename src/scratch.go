package main

import (
	"fmt"
	"math/rand"
	"sync"
)

func main() {
	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			fmt.Printf("%d\n", rand.Intn(150)+150)
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("%d", rand.Intn(150)+150)
	fmt.Printf("%d", rand.Intn(150)+150)
}
