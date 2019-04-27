package test_data

import (
	"context"
	"encoding/json"
	kafkaGo "github.com/segmentio/kafka-go"
	"kafka-to-elastic-pipeline/config"
	"kafka-to-elastic-pipeline/pkg/types"
)

func CreateUsersInKafka(num int32) error {
	kafkaUsersWriter := kafkaGo.NewWriter(kafkaGo.WriterConfig{
		Brokers: config.KafkaBrokers,
		Topic:   config.KafkaUsersTopic,
		Async:   true,
	})
	ctx := context.Background()

	usersChannel := make(chan types.User)
	controlChannel := make(chan struct{})

	go UsersIterator(usersChannel, controlChannel)

	counter := int32(0)
	for user := range usersChannel {
		binaryData, err := json.Marshal(user)
		if err != nil {
			return err
		}
		if err := write(ctx, kafkaUsersWriter, binaryData); err != nil {
			return err
		}
		counter++
		if counter == num {
			controlChannel <- struct{}{}
			break
		}
	}
	close(usersChannel)
	close(controlChannel)
	return kafkaUsersWriter.Close()
}

func CreateTweetsInKafka(num int32) error {
	kafkaTweetsWriter := kafkaGo.NewWriter(kafkaGo.WriterConfig{
		Brokers: config.KafkaBrokers,
		Topic:   config.KafkaTweetsTopic,
		Async:   true,
	})
	ctx := context.Background()

	tweetsChannel := make(chan types.Tweet)
	controlChannel := make(chan struct{})

	go TweetsIterator(tweetsChannel, controlChannel)

	counter := int32(0)
	for tweet := range tweetsChannel {
		binaryData, err := json.Marshal(tweet)
		if err != nil {
			return err
		}
		if err := write(ctx, kafkaTweetsWriter, binaryData); err != nil {
			return err
		}
		counter++
		if counter == num {
			controlChannel <- struct{}{}
			break
		}
	}
	close(tweetsChannel)
	close(controlChannel)
	return kafkaTweetsWriter.Close()
}

func write(ctx context.Context, kafkaWriter *kafkaGo.Writer, data []byte) error {
	msg := kafkaGo.Message{
		Value: data,
	}
	return kafkaWriter.WriteMessages(ctx, msg)
}
