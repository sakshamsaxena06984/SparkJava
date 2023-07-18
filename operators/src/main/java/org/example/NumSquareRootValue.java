package org.example;

import java.io.Serializable;

public class NumSquareRootValue implements Serializable {
    private Integer num;
    private Double ans;

    public NumSquareRootValue(Integer num) {
        this.num = num;
        this.ans = (Double) Math.sqrt(num);
    }
//    public Double SqaureRoot(){
//        return this.ans;
//    }
    public Double SqaureRoot(){
        return this.ans;
    }
    public void print(){
        System.out.println(this.num+"       "+this.ans);
    }

    @Override
    public String toString() {
        return "NumSquareRootValue{" +
                "num=" + num +
                ", ans=" + ans +
                '}';
    }
}
