package main.java;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.split("[^\\w]+")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((n1, n2) -> n1 + n2);
        counts.saveAsTextFile(args[1]);
        sc.close();
    }
}
