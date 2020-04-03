package main

import "sync"
//import "time"
//import "math/rand"

func main() {
	//rand.Seed(time.Now().UnixNano())

	//Some share data
	count := 0
	finished := 0
	//Some lock that protects that shared data and then
	var mu sync.Mutex
	//We have this condition variable that is given a pointer
	//to lock when it's initialized
	//Use this condition variable for kinkd of coordinating
	//when a certain condition some property on that shared data
	//when that becomes true
	//Modify code two places
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
			//When do something that changes the data
			//we call a conduct broadcast and we do this
			//while hoding the lock
			cond.Broadcast()
			//wakes up whoever's waiting on the condition variable:line 47
			//so this avoids having to that time dot sleep for some aribitrary amount of time
		}()
	}

  //The main thread grabs the lock it check this condition
	//suppose it's false ,it calls conduct wait
	mu.Lock()
	for count < 5 && finished != 10 {
		println("Wait")
		//On the other side where we're waiting for some condition
		//on that share data to become true
		//Be waiting it won't be like periodically waking up and checking some condition
		//that can't have changed because nobody else manipulated their share data
		cond.Wait()
	}

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
