package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class BoringWordCount{
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf1=new SparkConf().setAppName("StartSpark").setMaster("local[*]");
        JavaSparkContext sc1 = new JavaSparkContext(conf1);

        // will perform flatmap and filter together
        JavaRDD<String> initialRDD = sc1.textFile("D:\\SparkJava\\spark-java\\pair-rdd\\src\\main\\java\\org\\example\\input.txt");
        List<String> take = initialRDD.take(4);
//        take.forEach(e-> System.out.println(e));
        JavaRDD<String> map = initialRDD.map(s -> s.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> filter = map.filter(sentance -> sentance.trim().length() > 0);
        JavaRDD<String> stringJavaRDD = filter.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//        JavaRDD<String> filter1 = stringJavaRDD.filter(w -> Utils.isNotBoring(w)); // will check , why Utils class is not working
        stringJavaRDD.mapToPair(e-> new Tuple2<String,Long>(e,1L))
                      .reduceByKey((a,b)->a+b)
                      .mapToPair(t-> new Tuple2<>(t._2(),t._1()))
                      .sortByKey(false)
                      .coalesce(1)
                      .foreach(e-> System.out.println(e));

        System.out.println("---------after replace unnecessary alphabet with space----------");
//        stringJavaRDD.foreach(e-> System.out.println(e));


        sc1.close();


    }
}
