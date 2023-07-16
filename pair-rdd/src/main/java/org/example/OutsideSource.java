package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OutsideSource {
    @SuppressWarnings("resource")
    public static void main( String[] args ) throws Exception
    {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf1=new SparkConf().setAppName("StartSpark").setMaster("local[*]");
        JavaSparkContext sc1 = new JavaSparkContext(conf1);

        // will perform flatmap and filter together
        JavaRDD<String> initialRDD = sc1.textFile("D:\\SparkJava\\spark-java\\pair-rdd\\src\\main\\java\\org\\example\\input.txt");
        initialRDD.flatMap(e->Arrays.asList(e.split(" ")).iterator())
                        .foreach(e-> System.out.println(e));


        sc1.close();
    }
}
