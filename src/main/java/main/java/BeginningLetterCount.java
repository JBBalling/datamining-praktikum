package main.java;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * bin/spark-submit --class main.java.BeginningLetterCount /Users/jakobschwerter/Development/data-mining-praktikum/target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class BeginningLetterCount {

    public static void main(String[] args) throws Exception {
        String path = "/Users/jakobschwerter/Documents/Uni/Moderne Datenbanktechnologien/Praktikum - Data Mining/dataminingpraktikum-master/daten/pg100.txt";
        String output = "/Users/jakobschwerter/Documents/Uni/";
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(path);
        JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.split("[^\\w]+")).iterator());
        JavaRDD<String> wordsFiltered = words.filter(w -> (w.length() > 0 && Character.isLetter(w.charAt(0))));
        JavaPairRDD<Character, Integer> wordsLetters = wordsFiltered.mapToPair(w -> new Tuple2<Character, Integer>(w.toUpperCase().charAt(0), 1));
        JavaPairRDD<Character, Integer> counts = wordsLetters.reduceByKey((n1, n2) -> n1 + n2);
        counts.saveAsTextFile(output + "beginningLetterCount.txt");
        sc.close();
    }

}
