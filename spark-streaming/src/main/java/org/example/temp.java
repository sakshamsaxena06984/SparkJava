package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.*;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class temp {
    public static void main(String[] args) throws InterruptedException {
        System.out.println( "Hello World!" );
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf=new SparkConf().setMaster("local[*]").setAppName("viewingFigures");
        JavaStreamingContext sc=new JavaStreamingContext(conf, Durations.seconds(2));

//        Map<String,Object> props=new HashMap<>();
//        props.put("bootstrap.servers","localhost:9092");
//        props.put("key.deserializer",StringDeserializer.class);
//        props.put("value.deserializer",StringDeserializer.class);
//        props.put("group.id","prod");
//        props.put("auto.offset.reset","earliest");
//        Collection<String> topics=Arrays.asList("viewrecords");
//
//        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(sc,
//                LocationStrategies.PreferConsistent(),
//                ConsumerStrategies.<String, String>Subscribe(topics, props));
//        JavaDStream<String> results = directStream.map(e -> e.value());
//        results.print();
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("viewrecords");

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        sc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

        JavaDStream<String> map = messages.map(e -> e.value());
        map.print();



        sc.start();
     sc.awaitTermination();

    }
}
