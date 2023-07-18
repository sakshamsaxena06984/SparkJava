package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class MapOperator implements SparkCon {
    public static void main(String[] args) throws Exception {
        List<Integer> l=new ArrayList<>();
        l.add(Integer.valueOf(2));
        l.add(Integer.valueOf(6));
        l.add(Integer.valueOf(5));
        l.add(Integer.valueOf(2));
        l.add(Integer.valueOf(2));
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
           SparkConf conf1=new SparkConf().setAppName("StartSpark").setMaster("local[*]");
                  JavaSparkContext sc1 = new JavaSparkContext(conf1);
                  JavaRDD<Integer> myRdd = sc1.parallelize(l);
        Integer reduce = myRdd.reduce((a, b) -> (Integer) (a + b));
        System.out.println("value of l elements sum : "+reduce);

        //map operator
        JavaRDD<Double> mapResult=myRdd.map(v-> (Double) Math.sqrt(v));
//        mapResult.foreach(e-> System.out.println(e));
        mapResult.collect().forEach(w-> System.out.println(w));
        List<Double> collect = mapResult.collect();

        //no of element
        System.out.println("no of elements in rdd : "+mapResult.count());
        //another way of calculating
        JavaRDD<Long> q=myRdd.map(c-> (Long) 1L);
        Long count=q.reduce((c,c1)-> (Long) (c+c1));
        System.out.println("no of elements in rdd : "+count);


        /**
         * lecture12 : tuple
         */
        JavaRDD<NumSquareRootValue> mapR = myRdd.map(val -> new NumSquareRootValue(val));
        System.out.println("value of NumSquareRoot");
//        String ans=mapR.collect().toString();
//        System.out.println(ans);
//        System.out.println(mapR.collect().get(0).SqaureRoot());
       try {
           List<NumSquareRootValue> collect1 = mapR.collect();
           for(int i=0;i<collect1.size();i++){
               System.out.println(collect1.get(i).SqaureRoot());
           }
//           System.out.println("size of class elements : "+collect1.size());
//           collect1.forEach(e->e.SqaureRoot());

           //now, will use the tuple concept
           Tuple2<Integer,Double> tupleEx=new Tuple2<>(9,3.0);
           System.out.println(tupleEx._1()+" "+tupleEx._2());

           // in the rdd case
           JavaRDD<Tuple2<Integer,Double>> sqrtTuple=myRdd.map(e->new Tuple2<>(e,Math.sqrt(e)));

       }catch (Exception e){
           e.printStackTrace();
       }


        sc1.close();
    }
}
