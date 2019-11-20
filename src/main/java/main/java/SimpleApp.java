package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * bin/spark-submit --class main.java.SimpleApp /Users/jakobschwerter/Development/data-mining-praktikum/target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class SimpleApp {
    public static void main(String[] args) throws Exception {
        String path = "/Users/jakobschwerter/Documents/Uni/Moderne Datenbanktechnologien/Praktikum - Data Mining/dataminingpraktikum-master/daten/pg100.txt";
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.printf("%d lines\n", sc.textFile(path).count());
        sc.close();
    }
}
