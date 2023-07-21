package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;
import java.util.ArrayList;
import java.util.List;

public class DataFrameAPI {
    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("StartingSparkSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:\\C:\\tmp\\")
                .getOrCreate();
        try{

            // reading file
            Dataset<Row> dataset = spark.read().option("header", true).csv("D:\\SparkJava\\spark-java\\spark-sql\\src\\main\\resources\\demo1.csv");
           dataset=dataset.select(col("level"),
                  date_format(col("datetime"),"MMMM").as("month"),
                  date_format(col("datetime"),"M").as("m"));
           dataset.printSchema();
           dataset=dataset.groupBy(col("level"),col("month"),col("m")).count();
           dataset.orderBy(col("m")).show();

        }catch (Exception e){
            e.printStackTrace();

        }
        spark.close();

    }
}
