package main

import (
	"context"
	"log"
	"os"
	"sync"

	kafka "github.com/segmentio/kafka-go"
)

func startConsumer(groupId, topicId, consumerId string,
	closeCh <-chan os.Signal, wg *sync.WaitGroup) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     defaultBrokerAdds,
		GroupID:     groupId,
		Topic:       topicId,
		ErrorLogger: log.Default(),
		MaxAttempts: 1,
	})

	defer wg.Done()
	var exitLoop bool
	log.Printf("Started Consumer %v", consumerId)

	for {
		log.Printf("\n In for loop %v", consumerId)
		select {
		case <-closeCh:
			log.Printf("Gracefully Closing Consumer %v", consumerId)
			exitLoop = true
		default:
			log.Printf("Stuck %v", consumerId)
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Println("Error Occured while reading messages " + err.Error())
				exitLoop = true
				break
			}
			log.Printf("Released %v", consumerId)
			log.Printf("consumer=%v, topic=%v, part=%v, offset=%v, topic=%v, val=%v", consumerId, msg.Key, msg.Partition, msg.Offset, msg.Topic, string(msg.Value))

		}
		if exitLoop {
			break
		}
	}
	if err := reader.Close(); err != nil {
		log.Printf("Couldn't close the reader properly, %v", err)
	} else {
		log.Printf("Closed message reader %v", consumerId)
	}
}
