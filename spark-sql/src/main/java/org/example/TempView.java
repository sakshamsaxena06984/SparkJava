package org.example;

import org.apache.spark.sql.*;

public class TempView {
    public static void main(String[] args) {

        //JavaSparkContext sc=new JavaSparkContext(new SparkConf().setAppName("StartingSparkSql").setMaster("local[*]"));
        SparkSession spark=SparkSession.builder().appName("StartingSparkSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:\\C:\\tmp\\")
                .getOrCreate();
        try{


            System.out.println("hello, Spark-SQL");

            Dataset<Row> dataset = spark.read()
                    .option("header", true)
                    .csv("D:\\SparkJava\\spark-java\\spark-sql\\src\\main\\resources\\Sample.csv");

            dataset.show(30);

            //creating the view
            dataset.createTempView("student");
            Dataset<Row> sql = spark.sql("select * from student limit 10");
            sql.show();
            System.out.println("num of count  "+sql.count());

            spark.sql("select max(col1) from student").show();
            spark.sql("select sname, count(*) as count from student group by sname").show();


        }catch (Exception e){
            e.printStackTrace();

        }
        spark.close();

//        System.out.println( "Hello World!" );
    }
}
