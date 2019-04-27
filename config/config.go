package config

import "time"

// Topics and indexes
const (
	KafkaUsersTopic  = "users"
	KafkaTweetsTopic = "tweets"

	ESUsersIndex  = "users"
	ESTweetsIndex = "tweets"
)

var KafkaBrokers = []string{"localhost:9092"}
var ElasticAddress = "http://localhost:9200"

// Hard-coded path for simplicity. Shall be replaced by something like environment variable.
var GeoIPDBFile = "/home/max/GolandProjects/kafka-to-elastic-pipeline/assets/GeoLite2-City_20190312/GeoLite2-City.mmdb"

// Performance config
const (
	NumPartitionsKafkaUsersTopic  = 2
	NumPartitionsKafkaTweetsTopic = 10

	NumGeoIPWorkers    = 3
	NumElasticWriters  = 2
	ChannelsBufferSize = 100 // one setting for several channels, for simplicity

	ElasticWorkerBuffer        = 3000
	ElasticForcedFlushInterval = time.Second * 5
)
