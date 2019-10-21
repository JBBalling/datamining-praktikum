package main.java;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class BeginningLetterCount {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.split("[^\\w]+")).iterator());
        JavaRDD<String> wordsFiltered = words.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return (v1.length() > 0 && Character.isLetter(v1.charAt(0)));
            }
        });
        JavaPairRDD<Character, Integer> wordsLetters = wordsFiltered.mapToPair(w -> new Tuple2<Character, Integer>(w.toUpperCase().charAt(0), 1));
        JavaPairRDD<Character, Integer> counts = wordsLetters.reduceByKey((n1, n2) -> n1 + n2);
        counts.saveAsTextFile(args[1]);
        sc.close();
    }

}
