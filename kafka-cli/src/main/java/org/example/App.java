package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Hello world!
 *
 */
/*
configuration.properties:’
spring.kafka.consumer.bootstrap-servers: localhost:9092
spring.kafka.consumer-group-id:myGroup
spring.kafka.consumer.auto-offset-reset:earliest
spring.kafka.consumer.key.deserializer: org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value.deserializer: org.apache.kafka.common.serialization.StringDeserializer

spring.kafka.producer.bootstrap-servers:localhost:9092
spring.kafka.producer.key-serializer: org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer: org.apache.kafka.common.serialization.StringSerializer


 */
public class App 
{
    public static void main( String[] args ) throws FileNotFoundException, InterruptedException {

//        Properties props =new Properties();
//        props.put("spring.kafka.producer.bootstrap-servers","localhost:9092");
////        props.put("spring.kafka.consumer-group-id","myGroup");
////        props.put("spring.kafka.consumer.auto-offset-reset","earliest")
//        props.put("acks","all");
//        props.put("retries",0);
//        props.put("batch.size",16384);
//        props.put("linger.ms",1);
//        props.put("buffer.memory",33554432);
//        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");


        String bootstrapServers = "127.0.0.1:9092";
      Properties props=new Properties();
      props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
      props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
      props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        Producer<String,String> producer=new KafkaProducer<>(props);
        Scanner sc=new Scanner(new FileReader("D:\\SparkJava\\spark-java\\kafka-cli\\src\\main\\java\\org\\example\\final_viewingfigures"));
        int miliseconds=0;
        while(sc.hasNextLine()){
            String[] input= sc.nextLine().split(",");
            Integer timestamp = new Integer(input[0]);
            Integer courseKey = new Integer(input[1]);
            while(miliseconds<timestamp){
                miliseconds++;
                if(miliseconds%24==0){
                    Thread.sleep(100);
                }
            }
            String courseName=courseKeys.get(courseKey);
            producer.send(new ProducerRecord<String,String>("viewrecords",courseName));
        }
        sc.close();
        producer.close();



    }
    private static Map<Integer, String> courseKeys = Stream.of(new Object[][] {
            {0,"Spring Boot Microservices"},
            {1,"Spring Framework Fundamentals"},
            {2,"Spring JavaConfig"},
            {3,"Spring MVC and WebFlow"},
            {4,"JavaEE and WildFly Module 1 : Getting Started"},
            {5,"Hibernate and JPA"},
            {6,"Java Web Development Second Edition: Module 1"},
            {7,"Java Fundamentals"},
            {8,"NoSQL Databases"},
            {9,"Java Advanced Topics"},
            {10,"Docker for Java Developers"},
            {11,"Java Web Development Second Edition: Module 2"},
            {12,"HTML5 and Responsive CSS for Developers"},
            {13,"Git"},
            {14,"Spring Boot"},
            {15,"Groovy Programming"},
            {16,"Java Build Tools"},
            {17,"Hadoop for Java Developers"},
            {18,"Cloud Deployment with AWS"},
            {19,"Docker Module 2 for Java Developers"},
            {20,"Going Further with Android"},
            {21,"Test Driven Development"},
            {22,"Introduction to Android"},
            {23,"Java Web Development"},
            {24,"Spring Security Module 3"},
            {25,"Java Messaging with JMS and MDB"},
            {26,"Spring Remoting and Webservices"},
            {27,"Thymeleaf"},
            {28,"Spring Security Module 2: OAuth2 and REST"},
            {29,"JavaEE and WildFly Module 2: Webservices"},
            {30,"Spring Security Core Concepts"},
            {31,"JavaEE and Wildfly Module 3: Messaging"},
            {32,"JavaEE"},
            {33,"Microservice Deployment"},
            {34,"Securing a VPC"},
            {35,"WTP Plugins for Eclipse"},
            {36,"Spark for Java Developers"},
            {37,"JavaEE and Wildfly Module 4: JSF"},
            {38,"Kubernetes Microservices Module 1"},
            {39,"Kotlin with Spring Boot"},
            {40,"Kubernetes Microservices Module 2"},
            {41,"Spark Module 2 SparkSQL and DataFrames"},
            {42,"Spark Module 3 Machine Learning SparkML"}}).collect(Collectors.toMap(it -> (Integer)it[0], it -> (String)it[1]));

}
