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
			//Firstly, send request
			vote := requestVote()
			//then afer that grab lock and then update these shared variables
			mu.Lock()
			defer mu.Unlock()
			if vote {
				count++
			}
			finished++
		}()
	}

	//for count < 5 && finished != 10 {
	//Hint: the following codes is not as nice as it could be
	//Issue:the issue here is that it's busy waiting,what it's doing
	//is in a very tight loop, it's grabbing the lock ,checking This
	//condition ,unlocking grabbing this lock,
	//It's going to burn up 100% CPU on one core while it's doing This
	//So this code is correct but itls like at a high level we don't
	//care abuut efficiency like CPU  
	for  {
		//wait
		mu.Lock()
		if count >= 5 || finished == 10 {
			println("Break!count=" ,count,"finished=",finished)
			break
		}
		mu.Unlock()
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
