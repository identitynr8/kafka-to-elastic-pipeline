package kafka

import (
	"context"
	kafkaGo "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"kafka-to-elastic-pipeline/config"
	"kafka-to-elastic-pipeline/pkg/types"
	"kafka-to-elastic-pipeline/test/test_data"
	"log"
	"testing"
	"time"
)

func TestReadTweets(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	tweetChan := make(chan types.Tweet)

	tweetsReader := kafkaGo.NewReader(kafkaGo.ReaderConfig{
		Brokers:   config.KafkaBrokers,
		Partition: 0,
		Topic:     config.KafkaTweetsTopic,
		MinBytes:  1,
		MaxBytes:  10e6,
	})

	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to initilaize logger: %s", err)
	}

	// Create as many tweets in kafka as there are partitions.
	// Round-robin will put one tweet into partition 0
	if err := test_data.CreateTweetsInKafka(config.NumPartitionsKafkaTweetsTopic); err != nil {
		log.Fatalf("failed to create tweets in kafka: %s", err)
	}

	go ReadTweets(ctx, tweetsReader, tweetChan, logger)

	select {
	case tweet := <-tweetChan:
		if tweet.RemoteAddress == "" || tweet.Tags == nil || tweet.User == nil || tweet.Message == "" {
			t.Fatal("some tweet fields are not set")
		}
	case <-ctx.Done():
		t.Fatal("tweets reader didn't return data")
	}
}

func TestReadUsers(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	userChan := make(chan types.User)

	usersReader := kafkaGo.NewReader(kafkaGo.ReaderConfig{
		Brokers:   config.KafkaBrokers,
		Partition: 0,
		Topic:     config.KafkaUsersTopic,
		MinBytes:  1,
		MaxBytes:  10e6,
	})

	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to initilaize logger: %s", err)
	}

	if err := test_data.CreateUsersInKafka(config.NumPartitionsKafkaUsersTopic); err != nil {
		log.Fatalf("failed to create users in kafka: %s", err)
	}

	go ReadUsers(ctx, usersReader, userChan, logger)

	select {
	case user := <-userChan:
		if user.Id == "" || user.Name == "" {
			t.Fatal("some user fields are not set")
		}
	case <-ctx.Done():
		t.Fatal("users reader didn't return data")
	}
}

// Benchmark tweets reader. Users reader should perform more or less the same.
func BenchmarkReader(b *testing.B) {
	if err := test_data.CreateTweetsInKafka(int32(b.N * config.NumPartitionsKafkaTweetsTopic)); err != nil {
		log.Fatalf("failed to create tweets in kafka: %s", err)
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	tweetChan := make(chan types.Tweet)

	tweetsReader := kafkaGo.NewReader(kafkaGo.ReaderConfig{
		Brokers:   config.KafkaBrokers,
		Partition: 0,
		Topic:     config.KafkaTweetsTopic,
		MinBytes:  1,
		MaxBytes:  10e6,
	})

	logger, err := zap.NewDevelopment()
	if err != nil {
		b.Fatalf("Failed to initilaize logger: %s", err)
	}

	go ReadTweets(ctx, tweetsReader, tweetChan, logger)

	for i := 0; i < b.N; i++ {
		select {
		case <-tweetChan:
		case <-ctx.Done():
			b.Fatal("timed out")
		}
	}
}
