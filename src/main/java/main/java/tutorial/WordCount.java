package main.java.tutorial;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * sparkSubmit --class main.java.tutorial.WordCount target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        String path = "daten/pg100.txt";
        String output = "output/tutorial/wordCount";
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(path);
        JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.split("[^\\w]+")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((n1, n2) -> n1 + n2);
        counts.saveAsTextFile(output);
        sc.close();
    }
}
