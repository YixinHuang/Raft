package main

import "time"
import "math/rand"

//This pattern where whenever a Raft peer becomes a candidate
//it wants to send out vote requests all of ites followers
//and eventually the followers come back to the candidata and
//say yes or no whether or not the candidate got the vote right

//When we ask all peers in parallel
//We dont want to them wait so we get a response from all of
//them before making up our mind right,because if a candidat
//gets a majority of votes like it doesn't need to wait till
//it hears back from everybody

func main() {
	rand.Seed(time.Now().UnixNano())

	count := 0    //the number of majority to win leader
	finished := 0 //the number of responses we've gotten in total

  //So idea is we want to send out vote reuests in parallel
	//and keep track of how many yeses I've get and how many
	//responses I've gotten in general
	for i := 0; i < 10 ; i++ {
		go func() {
			vote := requestVote()
			if vote {
				count++
			}
			finished++
		}()
	}

	for count < 5 && finished != 10 {
		//wait
		//	println("for count=" ,count,"finished=",finished)
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
