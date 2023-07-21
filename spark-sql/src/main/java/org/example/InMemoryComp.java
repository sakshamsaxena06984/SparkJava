package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class InMemoryComp {
    public static void main(String[] args) {

        //JavaSparkContext sc=new JavaSparkContext(new SparkConf().setAppName("StartingSparkSql").setMaster("local[*]"));
        SparkSession spark=SparkSession.builder().appName("StartingSparkSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:\\C:\\tmp\\")
                .getOrCreate();
        try{
            List<Row> dataset=new ArrayList<>();
            dataset.add(RowFactory.create("WARN","2016-12-13 04:19:32"));

            StructField[] fields=new StructField[]{
                    new StructField("level", DataTypes.StringType,false, Metadata.empty()),
                    new StructField("datatime",DataTypes.StringType,false,Metadata.empty())
            };
            StructType schema= new StructType(fields);
            Dataset<Row> dataFrame = spark.createDataFrame(dataset, schema);
            dataFrame.show();

            dataFrame.createTempView("ep");
            spark.sql("select level,date_format(datatime,'yyy') from ep").show();
            spark.sql("select level,date_format(datatime,'MMMM') from ep").show();


            // reading file
            Dataset<Row> dt = spark.read().option("header", true).csv("D:\\SparkJava\\spark-java\\spark-sql\\src\\main\\resources\\demo1.csv");
            dt.show(10);
            dt.createTempView("tab");
            dt.printSchema();
            spark.sql("select * from tab limit 10").show();
            spark.sql("select level,date_format(datetime,'MMMM') as month," +
                    "count(1) as total from tab group by level,datetime").show();

            spark.sql("select level,date_format(datetime,'MMMM') as month,first(date_format(datetime,'M')) as m," +
                    "count(1) as total from tab group by level,month order by month").show();

            //casting
            spark.sql("select level,date_format(datetime,'MMMM') as month," +
                    "cast(first(date_format(datetime,'M')) as int) as m," +
                    "count(1) as total from tab group by level,month order by m").show();

        }catch (Exception e){
            e.printStackTrace();

        }
        spark.close();

//        System.out.println( "Hello World!" );
    }
}
