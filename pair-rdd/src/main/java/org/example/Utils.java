package org.example;

import jdk.jfr.internal.tool.Main;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/*
 byte[] array = new byte[100];
 try {
 InputStream input = new FileInputStream("input.txt");
 System.out.println("Available bytes in the file: " + input.available());

 // Read byte from the input stream
 input.read(array);
 System.out.println("Data read from the file: ");

 // Convert byte array into string
 String data = new String(array);
 System.out.println(data);

 // Close the input stream
 input.close();
 } catch (Exception e) {
 e.getStackTrace();
 }
 }
 */
public class Utils implements Serializable {
   private static Set<String> boring= new HashSet<String>();
   static {
       InputStream is = Main.class.getResourceAsStream("D:\\SparkJava\\spark-java\\pair-rdd\\src\\main\\java\\org\\example\\boring.txt");
       BufferedReader br = new BufferedReader(new InputStreamReader(is));
       br.lines().forEach(i->boring.add(i));
   }

   public static  boolean isBoring(String word){
       return boring.contains(word);
   }
   public static boolean isNotBoring(String word){
       return !isBoring(word);
   }
}
