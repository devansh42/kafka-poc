package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

func startProducer(topicId, producerId string,
	interval time.Duration,
	closeCh <-chan os.Signal,
	wg *sync.WaitGroup) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: defaultBrokerAdds,
		Topic:   topicId,
	})
	defer wg.Done()
	log.Printf("Started Producer with topic=%v, producerId=%v, interval=%v", topicId, producerId, interval)
	var exitLoop bool
	for {
		select {
		case <-closeCh:
			exitLoop = true
			log.Printf("Gracefully exiting the loop %v", producerId)
		case <-time.After(interval):
			i := strconv.Itoa(rand.Int())
			err := writer.WriteMessages(context.Background(), kafka.Message{
				Value: []byte(i),
			})
			if err != nil {
				log.Printf("Couldn't write message %v", err.Error())
				exitLoop = true
				break
			}
			log.Printf("Wrote the message to the producer %v", producerId)
		}
		if exitLoop {
			break
		}
	}
	if err := writer.Close(); err != nil {
		log.Printf("Couldn't close message writer %v", err.Error())
	} else {
		log.Printf("Closed message writer %v", producerId)
	}
}
