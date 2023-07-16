package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public interface SparkCon {
    SparkConf con=new SparkConf().setAppName("Op").setMaster("local[*]");
    SparkContext sc=new SparkContext(con);
}
