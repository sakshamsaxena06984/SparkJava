package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;

/**
 * Hello world!
 *
 */
public class App 
{
    @SuppressWarnings("resource")
    public static void main( String[] args )
    {
//        Logger.getLogger("org.apache").setLevel(Level.WARN);

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

           System.out.println("total number of records : "+dataset.count());
           Row first = dataset.first();
           System.out.println(first);
           Object id = first.getAs("id");
           System.out.println("object type in first.getAs: "+id);
           String _id=id.toString();
           System.out.println("String type  : "+_id);
           int idd = Integer.parseInt(first.getAs("id"));
           System.out.println("integer type of id  : "+idd);

           //using of filtering
           Dataset<Row> filter = dataset.filter(" id= '4' ");
           filter.show();

           // filter using with lambda
        /*
          will check filter with lambda function
         */

       }catch (Exception e){
           e.printStackTrace();

       }
        spark.close();

//        System.out.println( "Hello World!" );
    }
}
