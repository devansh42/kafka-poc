package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
)

func main() {
	fmt.Printf("\nKafKa POC")
	var (
		closeCh     = make(chan os.Signal)
		interruptCh = make(chan os.Signal)
	)
	signal.Notify(interruptCh, os.Interrupt)
	var wg sync.WaitGroup
	wg.Add(8)

	go func() {
		defer wg.Done()
		<-interruptCh
		signal.Stop(interruptCh)
		close(closeCh)
	}()
	rand.Seed(time.Now().Unix())
	topicIdSuffix := strconv.Itoa(rand.Int())
	grpSuffix := strconv.Itoa(rand.Int())

	createTopic("A-" + topicIdSuffix)
	createTopic("B-" + topicIdSuffix)

	go startProducer("A-"+topicIdSuffix, "P1", time.Millisecond*100, closeCh, &wg)
	go startProducer("B-"+topicIdSuffix, "P2", time.Millisecond*50, closeCh, &wg)
	go startProducer("A-"+topicIdSuffix, "P3", time.Millisecond*10, closeCh, &wg)
	go startProducer("B-"+topicIdSuffix, "P4", time.Millisecond*500, closeCh, &wg)
	go startConsumer("G1-"+grpSuffix, "A-"+topicIdSuffix, "C1", closeCh, &wg)
	go startConsumer("G2-"+grpSuffix, "B-"+topicIdSuffix, "C2", closeCh, &wg)
	go startConsumer("G1-"+grpSuffix, "A-"+topicIdSuffix, "C3", closeCh, &wg)

	wg.Wait()
}
