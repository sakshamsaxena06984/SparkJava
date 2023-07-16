package org.example;

import org.apache.spark.SparkConf;

public class SparkContext {
   static org.apache.spark.SparkContext fun(){
        SparkConf conf=new SparkConf().setAppName("OperatorSpark").setMaster("local[*]");
        org.apache.spark.SparkContext sc=new org.apache.spark.SparkContext(conf);
        return sc;
   }

}
