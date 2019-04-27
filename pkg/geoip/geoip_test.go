package geoip

import (
	"context"
	"fmt"
	"github.com/oschwald/maxminddb-golang"
	"go.uber.org/zap"
	"kafka-to-elastic-pipeline/config"
	"kafka-to-elastic-pipeline/pkg/types"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestFetcher(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	geoIPReader, err := maxminddb.Open(config.GeoIPDBFile)
	if err != nil {
		t.Fatalf("Failed to open GeoIP reader: %s", err)
	}
	defer geoIPReader.Close()

	tweetCh := make(chan types.Tweet)
	enrichedTweetCh := make(chan types.EnrichedTweet)

	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to initilaize logger: %s", err)
	}

	go Fetcher(ctx, geoIPReader, tweetCh, enrichedTweetCh, logger)

	fetcherIsAlive := false
	select {
	case tweetCh <- types.Tweet{RemoteAddress: "213.113.90.242"}:
		fetcherIsAlive = true
	case enrichedTweetCh := <-enrichedTweetCh:
		want := map[string]string{
			"de":    "Stockholm",
			"en":    "Stockholm",
			"es":    "Estocolmo",
			"fr":    "Stockholm",
			"ja":    "ストックホルム",
			"pt-BR": "Estocolmo",
			"ru":    "Стокгольм",
			"zh-CN": "斯德哥尔摩",
		}
		if !reflect.DeepEqual(enrichedTweetCh.City, want) {
			t.Fatalf("uneexpected result; got %s, want %s", enrichedTweetCh.City, want)
		}

		want = map[string]string{
			"de":    "Schweden",
			"en":    "Sweden",
			"es":    "Suecia",
			"fr":    "Suède",
			"ja":    "スウェーデン王国",
			"pt-BR": "Suécia",
			"ru":    "Швеция",
			"zh-CN": "瑞典",
		}
		if !reflect.DeepEqual(enrichedTweetCh.Country, want) {
			t.Fatalf("uneexpected result; got %s, want %s", enrichedTweetCh.Country, want)
		}
	case <-ctx.Done():
		if fetcherIsAlive {
			t.Fatal("geoip fetcher didn't return")
		} else {
			t.Fatal("geoip fetcher didn't read from source channel")
		}
	}
}

func BenchmarkFetcher(b *testing.B) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	geoIPReader, err := maxminddb.Open(config.GeoIPDBFile)
	if err != nil {
		b.Fatalf("Failed to open GeoIP reader: %s", err)
	}
	defer geoIPReader.Close()

	tweetCh := make(chan types.Tweet)
	enrichedTweetCh := make(chan types.EnrichedTweet)

	logger, err := zap.NewDevelopment()
	if err != nil {
		b.Fatalf("Failed to initilaize logger: %s", err)
	}

	go Fetcher(ctx, geoIPReader, tweetCh, enrichedTweetCh, logger)

	for i := 0; i < b.N; i++ {
		tweetCh <- types.Tweet{
			RemoteAddress: fmt.Sprintf("%d.%d.%d.%d", rand.Intn(255), rand.Intn(255), rand.Intn(255), rand.Intn(255)),
		}
		select {
		case <-enrichedTweetCh:
		case <-ctx.Done():
			b.Fatal("timed out")
		}
	}
}
