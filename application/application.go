package application

import (
	"context"
	"github.com/elastic/go-elasticsearch"
	"github.com/oschwald/maxminddb-golang"
	kafkaGo "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"kafka-to-elastic-pipeline/config"
	"kafka-to-elastic-pipeline/pkg/geoip"
	"kafka-to-elastic-pipeline/pkg/monitor"
	"kafka-to-elastic-pipeline/pkg/readers/kafka"
	"kafka-to-elastic-pipeline/pkg/types"
	"kafka-to-elastic-pipeline/pkg/writers/elastic"
)

func Application() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	geoIPReader, err := maxminddb.Open(config.GeoIPDBFile)
	if err != nil {
		logger.Fatal("Failed to open GeoIP reader")
	}
	defer geoIPReader.Close()

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		logger.Fatal("Error creating the client: %s", zap.Error(err))
	}

	group, ctx := errgroup.WithContext(context.Background())

	usrChan := make(chan types.User, config.ChannelsBufferSize)
	tweetsChan := make(chan types.Tweet, config.ChannelsBufferSize)
	enrichedTweetsChan := make(chan types.EnrichedTweet, config.ChannelsBufferSize)

	for i := 0; i < config.NumPartitionsKafkaUsersTopic; i++ {
		usersReader := kafkaGo.NewReader(kafkaGo.ReaderConfig{
			Brokers:   config.KafkaBrokers,
			Partition: i,
			Topic:     config.KafkaUsersTopic,
			MinBytes:  1, // in dev we want to read up to every single byte from kafka (for tests reliability);
			MaxBytes:  10e6,
		})

		group.Go(func() error {
			return kafka.ReadUsers(ctx, usersReader, usrChan, logger)
		})
	}
	for i := 0; i < config.NumPartitionsKafkaTweetsTopic; i++ {
		tweetsReader := kafkaGo.NewReader(kafkaGo.ReaderConfig{
			Brokers:   config.KafkaBrokers,
			Partition: i,
			Topic:     config.KafkaTweetsTopic,
			MinBytes:  1, // in dev we want to read up to every single byte from kafka (for tests reliability);
			MaxBytes:  10e6,
		})
		group.Go(func() error {
			return kafka.ReadTweets(ctx, tweetsReader, tweetsChan, logger)
		})
	}

	for i := 0; i < config.NumGeoIPWorkers; i++ {
		group.Go(func() error {
			return geoip.Fetcher(ctx, geoIPReader, tweetsChan, enrichedTweetsChan, logger)
		})
	}

	for i := 0; i < config.NumElasticWriters; i++ {
		group.Go(func() error {
			return elastic.Write(ctx, es, usrChan, enrichedTweetsChan, logger)
		})
	}

	group.Go(func() error {
		return monitor.MonitorFillness(ctx, usrChan, tweetsChan, enrichedTweetsChan, logger)
	})

	logger.Info("...program started")
	if err := group.Wait(); err != nil {
		logger.Fatal("error has happened", zap.Error(err))
	}
}
