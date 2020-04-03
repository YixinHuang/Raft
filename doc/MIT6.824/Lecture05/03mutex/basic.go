package main

import "sync"
import "time"

func main() {
	counter := 0
	var mu sync.Mutex
	for i := 0; i < 1000; i++ {
		go func() {
			mu.Lock()
			//defer like scheduling this to run at the end of
			//the current function body
			defer mu.Unlock()
			//This is really common parttern  you'll see
			//for example in your RPC handlers for manipulate
			//either read or write data on the Raft structure
			//right and those update should be sychronized with
			//other concurrently happening updates
			//So often times the pattern for RPC handles would be like
			//grab the lock, deffer unlock and then go do some work in side 
			counter = counter + 1
		}()
	}

	time.Sleep(1 * time.Second)
	mu.Lock()
	println(counter)
	mu.Unlock()
}
