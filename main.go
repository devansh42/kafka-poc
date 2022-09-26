package main

import (
	"fmt"
	"os"
	"os/signal"
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
	go startProducer("A", "P1", time.Millisecond*100, closeCh, &wg)
	go startProducer("B", "P2", time.Millisecond*50, closeCh, &wg)
	go startProducer("A", "P3", time.Millisecond*10, closeCh, &wg)
	go startProducer("B", "P4", time.Millisecond*500, closeCh, &wg)
	go startConsumer("G1", "A", "C1", closeCh, &wg)
	go startConsumer("G2", "B", "C2", closeCh, &wg)
	go startConsumer("G1", "A", "C3", closeCh, &wg)

	wg.Wait()
}
