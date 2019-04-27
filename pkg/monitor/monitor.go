package monitor

import (
	"context"
	"go.uber.org/zap"
	"kafka-to-elastic-pipeline/pkg/types"
	"time"
)

// Monitors fillness of channels. High fillness means that sink layer is slower than source layer.
func MonitorFillness(ctx context.Context, usersChan chan types.User, tweetsChan chan types.Tweet, enrichedTweetsChan chan types.EnrichedTweet, logger *zap.Logger) error {
	tickChannel := time.NewTicker(time.Second * 10).C

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-tickChannel:
			logger.Info(
				"Channels fillness %",
				zap.Float32("users", 100*float32(len(usersChan))/float32(cap(usersChan))),
				zap.Float32("tweets", 100*float32(len(tweetsChan))/float32(cap(tweetsChan))),
				zap.Float32("enriched tweets", 100*float32(len(enrichedTweetsChan))/float32(cap(enrichedTweetsChan))),
			)
		}
	}
}
