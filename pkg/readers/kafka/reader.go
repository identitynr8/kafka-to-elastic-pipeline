package kafka

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"kafka-to-elastic-pipeline/pkg/types"
)

func ReadUsers(ctx context.Context, kafkaReader *kafka.Reader, sinkChannel chan types.User, logger *zap.Logger) error {
	for {
		message, err := kafkaReader.ReadMessage(ctx)
		if err != nil {
			logger.Error("failed to read", zap.Error(err))
			// the reader is closed
			return err
		}

		var user types.User
		err = json.Unmarshal(message.Value, &user)
		if err != nil {
			logger.Error("failed to decode message", zap.Error(err))
			return err
		}

		sinkChannel <- user
	}
}

func ReadTweets(ctx context.Context, kafkaReader *kafka.Reader, sinkChannel chan types.Tweet, logger *zap.Logger) error {
	for {
		message, err := kafkaReader.ReadMessage(ctx)
		if err != nil {
			// the reader is closed
			return err
		}

		var tweet types.Tweet
		err = json.Unmarshal(message.Value, &tweet)
		if err != nil {
			logger.Error("failed to decode message", zap.Error(err))
			return err
		}

		sinkChannel <- tweet
	}
}
