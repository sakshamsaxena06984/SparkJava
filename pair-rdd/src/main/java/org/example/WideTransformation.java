package org.example;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class WideTransformation {
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf1=new SparkConf().setAppName("StartSpark").setMaster("local[*]");
        JavaSparkContext sc1 = new JavaSparkContext(conf1);
        JavaRDD<String> initialRDD = sc1.textFile("D:\\SparkJava\\spark-java\\pair-rdd\\src\\main\\java\\org\\example\\input1.txt");
        System.out.println("number of partition at level:1 "+initialRDD.getNumPartitions());
        JavaPairRDD<String, String> stringStringJavaPairRDD = initialRDD.mapToPair(e -> {
            String[] arr = e.split(":");
            String level = arr[0];
            String date = arr[1];
            return new Tuple2<>(level, date);
        });

        System.out.println("number of partition at level:2 "+stringStringJavaPairRDD.getNumPartitions());

        JavaPairRDD<String, Iterable<String>> stringIterableJavaPairRDD = stringStringJavaPairRDD.groupByKey();


        // using caching concept
//        stringIterableJavaPairRDD=stringIterableJavaPairRDD.cache();

        //using of persist concept
        stringIterableJavaPairRDD=stringIterableJavaPairRDD.persist(StorageLevel.MEMORY_AND_DISK());
        stringIterableJavaPairRDD.foreach(e->{
            System.out.println("key: "+e._1+" has iteration "+ Iterables.size(e._2)+" numbers of elements");
        });

        System.out.println("number of partion at level:3 "+stringIterableJavaPairRDD.getNumPartitions());
        System.out.println("number of records: "+stringIterableJavaPairRDD.count());


        Scanner s=new Scanner(System.in);
        s.nextLine();





//        stringJavaRDD.foreach(e-> System.out.println(e));


        sc1.close();


    }
}
