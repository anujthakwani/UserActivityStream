package com.demo.constants;


public class KafkaConstants {
    public static String KAFKA_BROKER_STRING =
            "localhost:9092";
    public static String KAFKA_KEY_SERIALIZER =
            "org.apache.kafka.common.serialization.StringSerializer";
    public static String KAFKA_VALUE_SERIALIZER =
            "org.apache.kafka.connect.json.JsonSerializer";

    public  static  String DATE_FORMAT="yyyy-MM-dd HH:mm:ss";
    public  static  String DEFAULT_DATE="1970-01-01 00:00:00";
    public static String FILEPATH="/Users/athakwani/Desktop/sampleData";

    public  static  String TARGETTOPIC="user";
    public  static String SESSIONSTOPIC="userSessions";
    public static String STREAMAPPNAME="session-windows-example";
    public static String STREAMAPPCLIENTNAME="session-windows-example-client";

    public static String STATEDIR="/tmp/kafka-streams";
    public static String STREAMOFFSET="earliest";
    }
