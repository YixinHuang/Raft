package main

import "time"
import "sync"

var done bool
var mu sync.Mutex

func main() {
	time.Sleep(1 * time.Second)
	println("Started")
	go periodic()
	time.Sleep(5 * time.Second) //wait for a while so we can observe what ticker does
	println("Lock done and set it to true")	
	mu.Lock()
	done = true
	mu.Unlock()
	println("Cancelled")
	time.Sleep(3 * time.Second) //observe no output
	println("Exit from  main")
}

func periodic(){
	i := 0
	//for !rf.killed() {}
	for {
		println("tick")
		time.Sleep(1 * time.Second)
		mu.Lock()
		if done {
			mu.Unlock()
			println("return from periodic. done =",done)
			return
		}
		mu.Unlock()
		i = i + 1
		println("Loop in periodic times:",i)
	}
}
