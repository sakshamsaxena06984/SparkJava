package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
//        System.out.println( "Hello World!" );
        List<Tuple2<Integer,Integer>> t1=new ArrayList<>();
        t1.add(new Tuple2<>(1,111));
        t1.add(new Tuple2<>(2,222));
        t1.add(new Tuple2<>(3,333));
        t1.add(new Tuple2<>(4,444));

        List<Tuple2<Integer,String>> t2=new ArrayList<>();
        t2.add(new Tuple2<>(1,"java"));
        t2.add(new Tuple2<>(2,"java_script"));
        t2.add(new Tuple2<>(5,"python"));
        t2.add(new Tuple2<>(7,"ruby"));
        

//        SparkContext sc2=new SparkContext(new SparkConf().setAppName("Joins").setMaster("local[*]"));
        JavaSparkContext sc2=new JavaSparkContext(new SparkConf().setAppName("Joins").setMaster("local[*]"));
        JavaPairRDD<Integer, Integer> table1 = sc2.parallelizePairs(t1);
        JavaPairRDD<Integer, String> table2 = sc2.parallelizePairs(t2);
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = table2.join(table1);
        join.foreach(e-> System.out.println(e));

        //left join
        System.out.println("------------left join--------");
        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> integerTuple2JavaPairRDD = table1.leftOuterJoin(table2);
        integerTuple2JavaPairRDD.foreach(e-> {
            System.out.println(e._1+" "+ e._2._2.orElse("NULL").toUpperCase());
        });

        System.out.println("------------right join----------");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> integerTuple2JavaPairRDD1 = table1.rightOuterJoin(table2);
        integerTuple2JavaPairRDD1.foreach(e->{
            System.out.println(e._1+" "+e._2);
        });

        System.out.println("-----------full outer join-------");
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesian = table1.cartesian(table2);
        cartesian.foreach(e->{
            System.out.println("first-section "+e._1._1+ " - "+e._1._2+ " second-section "+e._2._1+" - "+e._2._2);
        });


        sc2.close();
    }
}
