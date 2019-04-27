package geoip

import (
	"context"
	"github.com/oschwald/maxminddb-golang"
	"go.uber.org/zap"
	"kafka-to-elastic-pipeline/pkg/types"
	"net"
)

// We don't want the whole geo data, but just city and country names
type geoAddress struct {
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
	Country struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"country"`
}

func Fetcher(ctx context.Context, reader *maxminddb.Reader, tweetChannel chan types.Tweet, enrichedTweetChannel chan types.EnrichedTweet, logger *zap.Logger) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case inTweet := <-tweetChannel:
			enrichedTweet := types.EnrichedTweet{
				Message:       inTweet.Message,
				User:          inTweet.User,
				Tags:          inTweet.Tags,
				RemoteAddress: inTweet.RemoteAddress,
			}

			ip := net.ParseIP(inTweet.RemoteAddress)

			var geoAddr geoAddress
			err := reader.Lookup(ip, &geoAddr)
			if err != nil {
				logger.Warn("failed to get geoip data", zap.Error(err))
			}

			enrichedTweet.City = geoAddr.City.Names
			enrichedTweet.Country = geoAddr.Country.Names

			enrichedTweetChannel <- enrichedTweet
		}
	}
}
