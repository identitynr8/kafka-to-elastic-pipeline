This program is my exercise in writing concurrent and performant Go code.
It simulates processing pipeline, that gets input data from Kafka,
augments it, and saves into elasticsearch.  

There are two types of messages that the program reads from Kafka:

```
type Tweet struct {
  Message string
  User *User
  Tags []string
  RemoteAddress string
}
```
and 
```
type User struct {
  Name string
  Id string
}
```
Tweets messages are augmented with geoIP data before being saved to elastic.
Users messages are saved to elastic as is.

## Implementation
The program is made of 3 types of "bricks" (goroutines):

- readers from Kafka;

- geoIP data fetchers;

- writers to Elasticsearch.

Bricks communicate with other bricks by means of 3 channels:

- users channel;

- tweets channel;

- enhanced tweets channel;

Here is communication pipeline:
```
[Kafka readers]
 |       |  
 |       |  Tweets channel 
 |       |------------------> [geoIP data fetchers]
 |                                     |
 |                                     | Enhanced tweets channel
 |                                     |
 | Users channel                       ↓
 |----------------------->  [Writers to Elasticsearch]                
```
Number of bricks of each type is determined by desire to have maximal performance at
minimal resources allocated.

To figure out actual numbers of readers, fetchers and writers, there are integration test (`TestIntegration`), that
measures total run time, and monitoring tool (`MonitorFillness`) that measures channels fillness. High fillness
means that sink layer is slower than source layer. Example:

- If fillness of users channel if high, it means that Kafka readers are faster than writers
to Elasticsearch. Then to get maximum performance one could try to increase number of writers,
but if it doesn't help (elastic service itself is a bottleneck), one could decrease number
of Kafka readers to save resources while keeping the same overall performance. 

So the approach that I had was to run the program with big numbers of goroutines of each type,
and with integration test and channels monitoring cut those numbers so that performance of what is remained matches performance of 
the bottleneck. On my laptop the bottleneck happened to be Elasticsearch service and eventually I ended up with 7.3K
messages transferred from Kafka to ES per second - with 3 geoIP fetchers 2 Elasticsearch writers and 12 Kafka readers
(I wanted to simulate potential prod setup and partitioned Kafka topics: 2 partitions for users, and 10 for tweets,
so number of Kafka readers was static). 

All the services (bricks) of the application live in one error group. If one service returns error, the whole group
is being cancelled. This is done on purpose. Services can survive over network glitches, they reconnect and they heal, and 
if they do return error, it is "serious" error that require human intervention.