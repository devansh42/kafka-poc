package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

var (
	defaultBrokerAdds = []string{"localhost:9092"}
)

func createTopic(topicId string) {
	conn, err := kafka.DialContext(context.Background(), "tcp", "localhost:9092")
	if err != nil {
		log.Printf("Couldn't connect to leader to create topic %v due to %v", topicId, err.Error())
		return
	}
	defer conn.Close()
	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topicId,
		NumPartitions:     2,
		ReplicationFactor: 1,
	})
	if err != nil {
		log.Printf("Couldn't create the topics due to %v", err)
		return
	}
}
