package main

import "sync"
import "time"
import "math/rand"

func main() {
	rand.Seed(time.Now().UnixNano())

	count := 0
	finished := 0
	var mu sync.Mutex

	for i := 0; i < 10 ; i++ {
		go func() {
			vote := requestVote()
			mu.Lock()
			defer mu.Unlock()
			if vote {
				count++
			}
			finished++
		}()
	}

//Simple solution: add wait fo 50 millisecond 
	for count < 5 && finished != 10 {
		//wait
		mu.Lock()
		if count >= 5 || finished == 10 {
				println("Break! count=" ,count,"finished=",finished)
			break
		}
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	if count >=  5 {
		println("Received 5+ votes! count=" ,count,"finished=",finished)
	} else {
		println("Lost")
	}
}

func requestVote()(vote bool){
	println("request vote")
	vote  = true
	return
}
