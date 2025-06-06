package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

/**
 * Hello world!
 *
 */
public class StructuredStreaming
{
    public static void main( String[] args ) throws TimeoutException, StreamingQueryException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession sc=SparkSession.builder()
                .master("local[*]")
                .appName("StructuredStreaming")
                .getOrCreate();

        Dataset<Row> df = sc.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .load();

        df.createOrReplaceTempView("viewing_figures");
        Dataset<Row> results = sc.sql("select value from viewing_figures");
        StreamingQuery query = results.writeStream().format("console")
                .outputMode(OutputMode.Append())
                .start();

        query.awaitTermination();

    }
}
