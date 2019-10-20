package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.printf("%d lines\n", sc.textFile(args[0]).count());
        sc.close();
    }
}
