package application_test

import (
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"io/ioutil"
	"kafka-to-elastic-pipeline/application"
	"kafka-to-elastic-pipeline/config"
	"kafka-to-elastic-pipeline/test/test_data"
	"log"
	"net/http"
	"os"
	"testing"
	"time"
)

const (
	testNumUsers  = 40e3
	testNumTweets = 400e3
)

func TestMain(m *testing.M) {

	kafkaConn, _ := kafka.Dial("tcp", config.KafkaBrokers[0])
	if err := kafkaConn.DeleteTopics(config.KafkaUsersTopic, config.KafkaTweetsTopic); err != nil {
		if err.Error() != "[3] Unknown Topic Or Partition: the request is for a topic or partition that does not exist on this broker" {
			log.Fatalf("failed to delete topics: %s", err)
		}
	}
	time.Sleep(time.Second * 5) // give kafka some time to delete topics for real

	err := kafkaConn.CreateTopics(
		kafka.TopicConfig{Topic: config.KafkaUsersTopic, NumPartitions: config.NumPartitionsKafkaUsersTopic, ReplicationFactor: 1},
		kafka.TopicConfig{Topic: config.KafkaTweetsTopic, NumPartitions: config.NumPartitionsKafkaTweetsTopic, ReplicationFactor: 1},
	)
	if err != nil {
		log.Fatalf("failed to create topics: %s", err)
	}
	if err := kafkaConn.Close(); err != nil {
		log.Fatal("failed to close connection to kafka")
	}

	if err := test_data.CreateUsersInKafka(testNumUsers); err != nil {
		log.Fatalf("failed to create users in kafka: %s", err)
	}
	if err := test_data.CreateTweetsInKafka(testNumTweets); err != nil {
		log.Fatalf("failed to create tweets in kafka: %s", err)
	}

	client := &http.Client{}
	for _, method := range []string{"DELETE", "PUT"} {
		for _, index := range []string{config.ESUsersIndex, config.ESTweetsIndex} {
			req, err := http.NewRequest(method, fmt.Sprintf("%s/%s", config.ElasticAddress, index), nil)
			if err != nil {
				log.Fatalf("failed to prepare request: %s", err)
			}
			resp, err := client.Do(req)
			if err != nil {
				log.Fatalf("failed to run request: %s", err)
			}
			_ = resp.Body.Close()
		}
	}

	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	go application.Application()

	currNumUsers := int32(0)
	currNumTweets := int32(0)
	lastMeasured := time.Now()
	mustChangeWithin := time.Second * 10

	startTime := time.Now()

	for {
		time.Sleep(time.Second * 5)
		resp, err := http.Get(fmt.Sprintf("%s/%s,%s/_stats/indexing", config.ElasticAddress, config.ESUsersIndex, config.ESTweetsIndex))
		if err != nil {
			t.Fatal(err)
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			t.Fatal(err)
		}

		indexedUsers := int32(result["indices"].(map[string]interface{})[config.ESUsersIndex].(map[string]interface{})["primaries"].(map[string]interface{})["indexing"].(map[string]interface{})["index_total"].(float64))
		indexedTweets := int32(result["indices"].(map[string]interface{})[config.ESTweetsIndex].(map[string]interface{})["primaries"].(map[string]interface{})["indexing"].(map[string]interface{})["index_total"].(float64))

		if indexedUsers == testNumUsers && indexedTweets == testNumTweets {
			// everything is good, the job is done
			// all the users and tweets are in ES index
			break
		}
		if indexedUsers == currNumUsers && indexedTweets == currNumTweets && time.Now().Unix() >= lastMeasured.Add(mustChangeWithin).Unix() {
			t.Fatal("application stalled")
		}
		currNumTweets = indexedTweets
		currNumUsers = indexedUsers
		lastMeasured = time.Now()
	}
	timeDone := time.Now()
	t.Logf("Time taken: %s; objects per second: %d", timeDone.Sub(startTime), int32((testNumTweets+testNumUsers)/timeDone.Sub(startTime).Seconds()))
}
