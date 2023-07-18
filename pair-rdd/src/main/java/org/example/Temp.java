package org.example;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 *
 *

 */
public class Temp {
    public static void main(String[] args) {
        byte[] arr = new byte[100];
        try{
            InputStream input=new FileInputStream("D:\\SparkJava\\spark-java\\pair-rdd\\src\\main\\java\\org\\example\\input.txt");
            System.out.println("checking in input : "+input.available());
            input.read(arr);
            System.out.println("Data read from the file : ");
            String data = new String(arr);
            System.out.println(data);

            input.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
