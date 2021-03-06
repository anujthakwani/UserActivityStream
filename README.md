# UserActivityStream

This is Kafka Streams App that sessionizes incoming user activity data. Here basic assumption is that all the events for a given user will go to same partition, so as to maintain it ordering. That is why we have used userId as key will producing messages in topic from UserProducer class.  

## Compile
```
mvn clean install
```


# How To Run


#### Running Producer:
```
java -cp UserActivityStream-1.0-SNAPSHOT-jar-with-dependencies.jar com.demo.UserProducer ./resource/sampleData
```
#### Running Consumer:
```
java -cp UserActivityStream-1.0-SNAPSHOT-jar-with-dependencies.jar com.demo.Consumer <topic-to-read> <consmer-group>

```

#### Running Streams:
```
java -cp UserActivityStream-1.0-SNAPSHOT-jar-with-dependencies.jar com.demo.streams.UserActivityStreamsDriver
```


# Notes:

```
-->Input Topic is user.
-->Please execute cmd '/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user' to create topic
-->Output Topic of streams is userSessions. This will contain sessionized events info.
-->sample input data is available in resources folder
```
