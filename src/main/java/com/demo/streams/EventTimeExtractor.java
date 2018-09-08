package com.demo.streams;

import com.demo.models.UserActivity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;

// Extracts the  timestamp from the field of a record .
public class EventTimeExtractor implements TimestampExtractor {

    SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Logger logger = Logger.getLogger(EventTimeExtractor.class);



    @Override
    public long extract(ConsumerRecord<Object, Object> record,long previousTimestamp) {
        try {
            // `UserActivity` is our own custom class, which is the pojo we use to produce messages in Kafka.

            UserActivity jsonNode = (UserActivity) record.value();



            if (jsonNode != null) {

                logger.info(String.format("returning timestamp from custom timeExtractor %s",sf.parse(jsonNode.getTs()).getTime()));
                return sf.parse(jsonNode.getTs()).getTime();
            } else {


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
