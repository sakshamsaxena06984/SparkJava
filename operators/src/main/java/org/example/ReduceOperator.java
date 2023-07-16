package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class ReduceOperator implements SparkCon {
    public static void main(String[] args) {
        List<Double> l=new ArrayList<>();
        l.add(2.3);
        l.add(6.3);
        l.add(5.3);
        l.add(2.35);
        l.add(22.3);

           SparkConf conf1=new SparkConf().setAppName("StartSpark").setMaster("local[*]");
                  JavaSparkContext sc1 = new JavaSparkContext(conf1);
                  JavaRDD<Double> myRdd = sc1.parallelize(l);
        Double reduce = myRdd.reduce((a, b) -> a + b);
        System.out.println("value of l elements sum : "+reduce);
        sc1.close();
    }
}
