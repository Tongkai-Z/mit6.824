package main

import (
	"encoding/json"
	"fmt"
	"os"

	"6.824/mr"
)

func main() {
	file3, _ := os.Create("mr-0-0")
	enc := json.NewEncoder(file3)

	kv := mr.KeyValue{"mesdf", "1"}
	enc.Encode(&kv)
	enc2 := json.NewEncoder(file3)
	kv = mr.KeyValue{"123123", "1"}
	enc2.Encode(&kv)
	file3.Close()
	kva := []mr.KeyValue{}
	f, _ := os.Open("mr-0-0")
	dec := json.NewDecoder(f)
	for {
		var kv mr.KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	fmt.Printf("%v", kva)
}
