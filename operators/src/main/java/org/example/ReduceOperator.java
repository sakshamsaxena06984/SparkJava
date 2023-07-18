package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class ReduceOperator implements SparkCon {
    public static void main(String[] args) {
        List<Double> l=new ArrayList<>();
        l.add(Double.valueOf(2.3));
        l.add(Double.valueOf(6.3));
        l.add(Double.valueOf(5.3));
        l.add(Double.valueOf(2.35));
        l.add(Double.valueOf(22.3));

           SparkConf conf1=new SparkConf().setAppName("StartSpark").setMaster("local[*]");
                  JavaSparkContext sc1 = new JavaSparkContext(conf1);
                  JavaRDD<Double> myRdd = sc1.parallelize(l);
        Double reduce = myRdd.reduce((a, b) -> (Double) (a + b));
        System.out.println("value of l elements sum : "+reduce);
        sc1.close();
    }
}
