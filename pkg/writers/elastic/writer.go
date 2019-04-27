package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	"go.uber.org/zap"
	"kafka-to-elastic-pipeline/config"
	"kafka-to-elastic-pipeline/pkg/types"
	"strings"
	"time"
)

type bufferEntity struct {
	esIndex string
	data    []byte
}

func Write(ctx context.Context, es *elasticsearch.Client, usersChannel chan types.User, tweetsChannel chan types.EnrichedTweet, logger *zap.Logger) error {
	tickChannel := time.NewTicker(config.ElasticForcedFlushInterval).C
	lastFlushed := time.Now()

	var buffer []bufferEntity
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-tickChannel:
			if lastFlushed.Add(config.ElasticForcedFlushInterval).Unix() <= time.Now().Unix() {
				// flush by tick signal only if last flash was at least `ElasticForcedFlushInterval` time ago
				buffer, lastFlushed = flush(ctx, buffer, es, logger)
			}

		case user := <-usersChannel:
			bytes, err := json.Marshal(user)
			if err != nil {
				return err
			}
			buffer = append(buffer, bufferEntity{esIndex: config.ESUsersIndex, data: bytes})

		case tweet := <-tweetsChannel:
			bytes, err := json.Marshal(tweet)
			if err != nil {
				return err
			}
			buffer = append(buffer, bufferEntity{esIndex: config.ESTweetsIndex, data: bytes})
		}

		if len(buffer) >= config.ElasticWorkerBuffer {
			buffer, lastFlushed = flush(ctx, buffer, es, logger)
		}
	}
}

func flush(ctx context.Context, buffer []bufferEntity, es *elasticsearch.Client, logger *zap.Logger) ([]bufferEntity, time.Time) {
	if len(buffer) > 0 {
		logger.Info(fmt.Sprintf("writing %d objects to ES", len(buffer)))
		var body strings.Builder
		for _, el := range buffer {
			body.WriteString(fmt.Sprintf("{\"index\" : { \"_index\" : \"%s\", \"_type\" : \"_doc\" }}\n", el.esIndex))
			body.WriteString(string(el.data) + "\n")
		}

		req := esapi.BulkRequest{
			Body:    strings.NewReader(body.String()),
			Refresh: "true", // this will make objects immediately searchable; convenient for dev, can be slow for prod
			Pretty:  false,
		}
		res, err := req.Do(ctx, es)
		if err != nil {
			logger.Error("error making bulk request", zap.Error(err))
		}

		if res.IsError() {
			logger.Error("error indexing tweet", zap.String("status", res.Status()))
		}
		if err := res.Body.Close(); err != nil {
			logger.Error("failed to close response body", zap.Error(err))
		}
		buffer = nil
	}
	return buffer, time.Now()
}
