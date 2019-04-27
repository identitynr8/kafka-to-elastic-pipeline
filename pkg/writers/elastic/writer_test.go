package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"kafka-to-elastic-pipeline/config"
	"kafka-to-elastic-pipeline/pkg/types"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestWrite(t *testing.T) {
	usersCh := make(chan types.User)
	enrichedTweetsCh := make(chan types.EnrichedTweet)

	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to initilaize logger: %s", err)
	}

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		t.Fatalf("Error creating the client: %s", err)
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, config.ElasticForcedFlushInterval+time.Second*5)
	defer cancel()

	go Write(ctx, es, usersCh, enrichedTweetsCh, logger)

	rand.Seed(time.Now().Unix())
	user := types.User{Name: fmt.Sprintf("User%f", rand.Float64())}
	foundUser := false

	tweet := types.EnrichedTweet{Message: fmt.Sprintf("Message%f", rand.Float64())}
	foundTweet := false

	select {
	case usersCh <- user:
	case <-ctx.Done():
		t.Fatal("failed to send data to writer")
	}
	select {
	case enrichedTweetsCh <- tweet:
	case <-ctx.Done():
		t.Fatal("failed to send data to writer")
	}

	for {
		select {
		case <-ctx.Done():
			t.Fatal("data not found in ES (timeout)")
		default:
			if !foundUser {
				err, foundUser = findObject(ctx, es, config.ESUsersIndex, fmt.Sprintf(`{"match":{"Name":"%s"}}`, user.Name))
			}
			if !foundTweet {
				err, foundTweet = findObject(ctx, es, config.ESTweetsIndex, fmt.Sprintf(`{"match":{"Message":"%s"}}`, tweet.Message))
			}
			if foundUser && foundTweet {
				return
			}
			time.Sleep(time.Second)
		}
	}
}

func BenchmarkWrite(b *testing.B) {
	usersCh := make(chan types.User)
	enrichedTweetsCh := make(chan types.EnrichedTweet)

	logger, err := zap.NewDevelopment()
	if err != nil {
		b.Fatalf("Failed to initilaize logger: %s", err)
	}

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		b.Fatalf("Error creating the client: %s", err)
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, config.ElasticForcedFlushInterval*2+time.Second*5)
	defer cancel()

	go Write(ctx, es, usersCh, enrichedTweetsCh, logger)

	for i := 0; i < b.N; i++ {
		// We just write data to a source channel and hope it is written to ES
		// To have this benchmark reliable, there should be a test ensuring that actual writing is happening,
		// and b.N should be significantly larger than writer buffers.
		select {
		case <-ctx.Done():
			b.Fatal("timed out")
		case usersCh <- types.User{Name: "some user"}:
		}
	}
	if b.N < 5*config.ElasticWorkerBuffer {
		b.Log("benchmark is not to be trusted (too few writes)")
	} else {
		b.Log("benchmark is trustworthy (enough writes)")
	}
}

func findObject(ctx context.Context, es *elasticsearch.Client, index string, query string) (err error, found bool) {
	res, err := es.Search(
		es.Search.WithContext(ctx),
		es.Search.WithIndex(index),
		es.Search.WithBody(strings.NewReader(fmt.Sprintf(`{"query":%s}`, query))),
		es.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		err = errors.New("error reading response body")
		return
	}

	var r map[string]interface{}

	if err = json.NewDecoder(res.Body).Decode(&r); err != nil {
		return
	}
	numFound := int(r["hits"].(map[string]interface{})["total"].(float64))
	return err, numFound > 0
}
