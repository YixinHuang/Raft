package main

import "sync"
import "time"
import "fmt"

func main() {
	alice := 1000
	bob := 1000
	var mu sync.Mutex

	total := alice + bob

	go func() {
		for i :=0; i < 1000; i++ {
			mu.Lock()
			alice -= 1
			mu.Unlock()
			mu.Lock()
			bob += 1
			mu.Unlock()
		}		
	}()

	go func() {
		for i :=0 ; i < 1000; i++ {
			mu.Lock()
			bob -= 1
			mu.Unlock()
			mu.Lock()
			alice += 1
			mu.Unlock()
		}
	}()

	start := time.Now()
	for time.Since(start) < 1*time.Second {
		mu.Lock()
		if alice + bob != total{
			fmt.Printf("observed violation ,alice = %v, bob = %v, sum = %v \n",alice,bob,alice+bob)
		}
		mu.Unlock()
	}

}
