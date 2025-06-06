package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws InterruptedException {

        System.out.println( "Hello World!" );
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
       try{
           SparkConf conf=new SparkConf().setMaster("local[*]").setAppName("viewingFigures");
           JavaStreamingContext sc=new JavaStreamingContext(conf, Durations.seconds(1));

//        Collection<String> topics= Arrays.asList("viewrecords");
           Map<String, Object> kafkaParams = new HashMap<>();
           kafkaParams.put("bootstrap.servers", "localhost:9092");
           kafkaParams.put("key.deserializer", StringDeserializer.class);
           kafkaParams.put("value.deserializer", StringDeserializer.class);
           kafkaParams.put("group.id", "my-group6");
           kafkaParams.put("auto.offset.reset", "earliest");
           kafkaParams.put("enable.auto.commit", false);

           System.out.println("Kafka call");

           Collection<String> topics=Arrays.asList("viewrecords");
           JavaInputDStream<ConsumerRecord<Object, Object>> directStream = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent(),
                   ConsumerStrategies.Subscribe(topics, kafkaParams));
           JavaDStream<Object> map = directStream.map(e -> e.value());

           // consuming records on the basis of batching
//           JavaPairDStream<Object, Long> result = directStream.mapToPair(e -> new Tuple2<>(e.value(), 5L));
//           JavaPairDStream<Long, Object> longObjectJavaPairDStream = result.reduceByKey((x, y) -> x + y)
//                   .mapToPair(item -> item.swap())
//                   .transformToPair(rdd -> rdd.sortByKey());
//           longObjectJavaPairDStream.print();

           //consuming records on the basis of windows
           JavaPairDStream<Long, Object> longObjectJavaPairDStream = directStream.mapToPair(e -> new Tuple2<>(e.value(), 5l))
                   .reduceByKeyAndWindow((x, y) -> x + y,Durations.minutes(60),Durations.minutes(1))
                   .mapToPair(i -> i.swap())
                   .transformToPair(r -> r.sortByKey());

           longObjectJavaPairDStream.print();


           sc.start();
           sc.awaitTermination();
       }catch (Exception e){
           e.printStackTrace();
       }

    }
}
