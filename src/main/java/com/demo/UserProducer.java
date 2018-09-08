package com.demo;

import com.demo.constants.KafkaConstants;

import com.demo.models.UserActivity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;



public class UserProducer {
    public static void main(String[] args) throws  Exception {

        Logger logger = Logger.getLogger(UserProducer.class);
        //org.apache.log4j.BasicConfigurator.configure();

        String fileName=args[0];

        String sourceTopic=KafkaConstants.TARGETTOPIC;
        Properties  props = new Properties();
        props.put("bootstrap.servers", KafkaConstants.KAFKA_BROKER_STRING);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", KafkaConstants.KAFKA_KEY_SERIALIZER);
        props.put("value.serializer", KafkaConstants.KAFKA_VALUE_SERIALIZER);

        Producer<String, JsonNode> producer = new KafkaProducer<>(props);

        //read from file. Define path of ifle KafkaConstants.
        File file = new File(fileName);
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        StringBuffer stringBuffer = new StringBuffer();
        ObjectMapper objectMapper = new ObjectMapper();
        String line;

        System.out.println(logger.isInfoEnabled());
        while ((line = bufferedReader.readLine()) != null) {
            UserActivity mp = new UserActivity();
            mp.parseString(line);
                JsonNode jsonNode= objectMapper.valueToTree(mp);

            //produce message with userId as key, so that all events of one user goes to same partition.
            producer.send(new ProducerRecord<String , JsonNode>(sourceTopic, mp.getUserId(),jsonNode));




        }
        producer.close();
        fileReader.close();


    }
}
