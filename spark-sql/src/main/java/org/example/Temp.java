package org.example;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
public class Temp {
    public static void main(String[] args) {
        System.out.println("Hello Temp!");
        DateTimeFormatter udf= DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime hh=LocalDateTime.now();
        System.out.println("final answer : "+udf.format(hh));


    }
}
