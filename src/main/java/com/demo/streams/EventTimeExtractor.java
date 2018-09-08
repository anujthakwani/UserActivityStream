package com.demo.streams;

import com.demo.models.UserActivity;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;

// Extracts the  timestamp from the field of a record .
public class EventTimeExtractor implements TimestampExtractor {

    ObjectMapper mapper = new ObjectMapper();
    SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Logger logger = Logger.getLogger(EventTimeExtractor.class);
    Gson gson = new Gson();


    @Override
    public long extract(ConsumerRecord<Object, Object> record,long previousTimestamp) {
        try {
            // `UserActivity` is our own custom class, which is the pojo we use to produce messages in Kafka.

            UserActivity jsonNode = (UserActivity) record.value();



            if (jsonNode != null) {

                logger.info(String.format("returning timestamp from custom timeExtractor %s",sf.parse(jsonNode.getTs()).getTime()));
                return sf.parse(jsonNode.getTs()).getTime();
            } else {
                // Kafka allows `null` as message value.  How to handle such message values
                // depends on your use case.  In this example, we decide to fallback to
                // wall-clock time (= processing-time)
                // .

                logger.info("Payload is null, returning default timestamp");
                return System.currentTimeMillis();
            }
        }
        catch (ParseException e){
            e.printStackTrace();
            e.getMessage();
            return System.currentTimeMillis();
        }

    }



}
