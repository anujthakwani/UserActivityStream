

package com.demo.streams;

import com.demo.constants.KafkaConstants;
import com.demo.serializer.JsonDeserializer;
import com.demo.serializer.JsonSerializer;
import com.demo.models.UserActivity;
import com.demo.models.UserActivityState;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class UserActivityStreamsDriver {

    public static void main(String[] args) {

        Logger logger = Logger.getLogger(UserActivityStreamsDriver.class);

        JsonSerializer<UserActivityState> userActivityStateSerializer = new JsonSerializer<>();
        JsonDeserializer<UserActivityState> userActivityStateDeserializer = new JsonDeserializer<>(UserActivityState.class);
        JsonDeserializer<UserActivity> userActivityDeserializer = new JsonDeserializer<>(UserActivity.class);
        JsonSerializer<UserActivity> userActivitySerializer = new JsonSerializer<>();
        Serde<UserActivity> activitySerde = Serdes.serdeFrom(userActivitySerializer,userActivityDeserializer);
        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        Serde<String> stringSerde = Serdes.serdeFrom(stringSerializer,stringDeserializer);
        Serde<UserActivityState> collectorSerde = Serdes.serdeFrom(userActivityStateSerializer,userActivityStateDeserializer);


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, UserActivityState> source =(KStream<String, UserActivityState>) builder.stream(KafkaConstants.TARGETTOPIC, Consumed.with(Serdes.String(), activitySerde))
                // group by key so we can count by session windows
                .groupByKey(Serialized.with(Serdes.String(), activitySerde))
                // window by session
                .windowedBy(SessionWindows.with(TimeUnit.MINUTES.toMillis(30)).until(TimeUnit.MINUTES.toMillis(720)))
                // aggregate events by window and key
                .aggregate(
                        UserActivityState::new,
                        (aggKey, newMessage, userCollector) -> {
                            logger.debug("Kafka Streams Sessionizer: Adding Event");
                            userCollector.add(newMessage,logger);

                            return userCollector;
                        },
                        (aggKey, leftSessionBean, rightSessionBean) -> {
                             logger.info(String.format("Kafka Streams Sessionizer: Merging Sessions leftSession %s rightSession %s",leftSessionBean.toString(),rightSessionBean.toString()));
                            if(leftSessionBean.dateStart.equals(KafkaConstants.DEFAULT_DATE)) {
                                leftSessionBean=rightSessionBean;
                                return leftSessionBean;
                            }
                            return leftSessionBean;
                        },
                        Materialized.<String, UserActivityState, SessionStore<Bytes, byte[]>>as("stream-processor" + "-session-store").withKeySerde(Serdes.String()).withValueSerde(collectorSerde)
                ).toStream().filter((k,v)->v!=null)
                .map((key,value)->{

                    //i have included window start and end time for debugging purpose
                    return new KeyValue<>(key.key() + "@" + key.window().start() + "->" + key.window().end(), value);
                });

        source.to(KafkaConstants.SESSIONSTOPIC, Produced.with(Serdes.String(), collectorSerde));

         Properties config = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConstants.STREAMAPPNAME);
        config.put(StreamsConfig.CLIENT_ID_CONFIG, KafkaConstants.STREAMAPPCLIENTNAME);
        // Where to find Kafka broker(s).
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKER_STRING);
        config.put(StreamsConfig.STATE_DIR_CONFIG, KafkaConstants.STATEDIR);
        // Set to earliest so we don't miss any data that arrived in the topics before the process
        // started
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.STREAMOFFSET);
        // disable caching to see session merging
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        //register custom time extractor.
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractor.class);




        KafkaStreams streams=new KafkaStreams(builder.build(), config);
       // streams.cleanUp();
        streams.start();

    }


}
