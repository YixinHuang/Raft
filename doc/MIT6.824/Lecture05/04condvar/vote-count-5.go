package main

import "sync"
import "time"
import "math/rand"

func main() {
	rand.Seed(time.Now().UnixNano())

	count := 0
	finished := 0
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for i := 0; i < 10 ; i++ {
		go func() {
			vote := requestVote()
			mu.Lock()
			defer mu.Unlock()
			if vote {
				count++
			}
			finished++
			cond.Broadcast()
		}()
	}

	mu.Lock()
	for count < 5 && finished != 10 {
		println("Wait ! count=" ,count,"finished=",finished)
		cond.Wait()
	}

 /*for finished < 11 {
	cond.Wait()
 }*/

	if count >=  5 {
		println("Received 5+ votes! count=" ,count,"finished=",finished)
	} else {
		println("Lost")
	}
	mu.Unlock()
}

func requestVote()(vote bool){
	println("request vote")
	vote  = true
	return
}
