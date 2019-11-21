package main.java.tutorial;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * sparkSubmit --class main.java.tutorial.SimpleApp target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class SimpleApp {
    public static void main(String[] args) throws Exception {
        String path = "daten/pg100.txt";
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.printf("%d lines\n", sc.textFile(path).count());
        sc.close();
    }
}
