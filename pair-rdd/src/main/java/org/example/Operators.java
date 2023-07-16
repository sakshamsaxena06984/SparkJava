package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class Operators
{
    @SuppressWarnings("resource")
    public static void main( String[] args ) throws Exception
    {
//        System.out.println( "Hello World!" );
        List<String> input=new ArrayList<>();
        input.add("WARN: Tuesday 4");
        input.add("WARN: Tuesday 14");
        input.add("ERROR: Wednesday 56");
        input.add("FATAL: Tuesday 87");
        input.add("ERROR: Friday 4");


//        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
        SparkConf conf1=new SparkConf().setAppName("StartSpark").setMaster("local[*]");
        JavaSparkContext sc1 = new JavaSparkContext(conf1);
//        JavaRDD<String> myRdd = sc1.parallelize(input);
//        /*
//        JavaPairRDD<String, String> stringStringJavaPairRDD = myRdd.mapToPair(e -> {
//            String[] split = e.split(":");
//            String a = split[0];
//            String b = split[1];
//            return new Tuple2<>(a, b);
//        });
//         */
//        // we can write the above code in below one line
//        JavaPairRDD<String,String> stringStringJavaPairRDD = myRdd.mapToPair(e -> new Tuple2<>(e.split(":")[0],e.split(":")[1]));
//
//        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = stringStringJavaPairRDD.mapToPair(e -> {
//            return new Tuple2<>(e._1, 1);
//        });
//        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey((v1, v2) -> v1 + v1);
//        stringIntegerJavaPairRDD1.foreach(t->{
//            System.out.println(t._1+" corresponding value is "+t._2);
//        });
//

        //we can write above code in very short manner
//        sc1.parallelize(input)
//                        .mapToPair(e-> new Tuple2<>(e.split(":")[0],1L))
//                        .reduceByKey((e1,e2)->e1+e2)
//                        .foreach(t-> System.out.println(t._1+" corresponding values "+t._2));

        // group by clause
        /*
        sc1.parallelize(input)
                        .mapToPair(e-> new Tuple2<>(e.split(":")[0],1L))
                        .groupByKey()
                        .foreach(t-> System.out.println(t._1+" "+ Iterables.size(t._2)));
        sc1.close();

         */
        /*
        JavaRDD<String> parallelize = sc1.parallelize(input);
        JavaRDD<String> stringJavaRDD = parallelize.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        stringJavaRDD.foreach(e-> System.out.println(e));
        sc1.close();

         */
        // filter
        /*
        JavaRDD<String> parallelize = sc1.parallelize(input);
        JavaRDD<String> stringJavaRDD = parallelize.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        JavaRDD<String> filter = stringJavaRDD.filter(e -> e.length() > 1);
        filter.foreach(e-> System.out.println(e));

         */
        // will perform flatmap and filter together
        sc1.parallelize(input)
                        .flatMap(e->Arrays.asList(e.split(" ")).iterator())
                        .filter(e->e.length()>1)
                        .foreach(e-> System.out.println(e));
        sc1.close();
    }     
}
