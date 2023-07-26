package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class Aggregations {
    public static void main(String[] args) {
        //JavaSparkContext sc=new JavaSparkContext(new SparkConf().setAppName("StartingSparkSql").setMaster("local[*]"));
        SparkSession spark=SparkSession.builder().appName("StartingSparkSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:\\C:\\tmp\\")
                .getOrCreate();
        try{
            //USER DEFINE FUNCTION
            spark.udf().register("hasPassed",(String e)->e.equals("Barry French"),DataTypes.BooleanType);

            //Another user define function
            spark.udf().register("hassPassed1",(Double e)->{
                return e>=68.02;
            },DataTypes.BooleanType);


            System.out.println("hello, Spark-SQL");

            Dataset<Row> dataset = spark.read()
                    .option("header", true)
                    .option("inferSchema",true)
                    .csv("D:\\SparkJava\\spark-java\\spark-sql\\src\\main\\resources\\Sample.csv");

            dataset.printSchema();
            dataset.groupBy("sname").max("col3").show(10);

            // will use costing

            dataset.groupBy("sname")
                    .agg(max(col("rank").cast(DataTypes.IntegerType)).alias("max_rank"),
                            min(col("rank").cast(DataTypes.IntegerType)).alias("min_rank")).show();

            dataset.withColumn("passing",callUDF("hasPassed",col("sname"))).show();

            //using another user define function
            dataset.withColumn("PASS",callUDF("hassPassed1",col("col3"))).show();

            dataset.show(30);
            dataset.explain();


        }catch (Exception e){
            e.printStackTrace();

        }
        spark.close();
    }
}
