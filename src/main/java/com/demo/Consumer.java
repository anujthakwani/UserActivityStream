package com.demo;


import com.demo.constants.KafkaConstants;
import com.demo.models.UserActivity;
import com.demo.models.UserActivityState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;


public class Consumer {
    private static Scanner in;

    static  Logger logger = Logger.getLogger(Consumer.class);

    public static void main(String[] argv)throws Exception{
        if (argv.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n",
                    com.demo.Consumer.class.getSimpleName());
            System.exit(-1);
        }
        in = new Scanner(System.in);
        String topicName = argv[0];
        String groupId = argv[1];

        ConsumerThread consumerRunnable = new ConsumerThread(topicName,groupId);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread{
        private String topicName;
        private String groupId;
        private KafkaConsumer<String,JsonNode> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId){
            this.topicName = topicName;
            this.groupId = groupId;
        }
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKER_STRING);
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

            configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, JsonNode>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


            //Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, JsonNode> record : records) {
                        JsonNode jsonNode = record.value();
                        if(record.topic().equals(KafkaConstants.TARGETTOPIC))
                            logger.info(String.format("Message from kafka %s", mapper.treeToValue(jsonNode,UserActivity.class)));
                        else
                            logger.info(String.format("Message from kafka %s", mapper.treeToValue(jsonNode,UserActivityState.class)));
                    }
                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }
        public KafkaConsumer<String,JsonNode> getKafkaConsumer(){
            return this.kafkaConsumer;
        }
    }
}
