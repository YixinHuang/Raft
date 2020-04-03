package main

//High level: the chnnels are like a queue
//like synchroization primitive
//but they don't behave quite like queue in
//the 
func main() {
	done := make(chan bool)
	for i := 0; i < 5; i ++ {
		go func(x int) {
			sendRPC(x)
			done <- true
		}(i)
	}
	for i :=0 ; i < 5 ; i++ {
		<-done
	}
}

func sendRPC(i int) {
	println(i)
}
