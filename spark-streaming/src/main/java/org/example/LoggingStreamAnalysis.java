package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class LoggingStreamAnalysis {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        SparkConf conf=new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
        JavaStreamingContext sc=new JavaStreamingContext(conf, Durations.seconds(10));
        JavaReceiverInputDStream<String> inputStream = sc.socketTextStream("localhost", 8989);

        JavaDStream<String> result = inputStream.map(e -> e);
        //note : JavaDStream is same as RDD
//        result.print();
        JavaPairDStream<String, Long> stringLongJavaPairDStream = result.mapToPair(r -> new Tuple2<String, Long>(r.split(",")[0], 1L));
//        JavaPairDStream<String, Long> stringLongJavaPairDStream1 = stringLongJavaPairDStream.reduceByKey((x, y) -> x + y);
//        stringLongJavaPairDStream1.print();

        // using window concept
        JavaPairDStream<String, Long> stringLongJavaPairDStream1 = stringLongJavaPairDStream.reduceByKeyAndWindow((x, y) -> x + y, Durations.minutes(1));
        stringLongJavaPairDStream1.print();
        sc.start();
        sc.awaitTermination();

    }
}
