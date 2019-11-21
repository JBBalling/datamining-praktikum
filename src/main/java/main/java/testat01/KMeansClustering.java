package main.java.testat01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * sparkSubmit --class main.java.testat01.KMeansClustering target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class KMeansClustering {

    public static void main(String[] args) throws Exception {

        String path = "daten/documentsTfidf.txt";
        String output = "output/testat01/KMeansClustering";

        SparkConf conf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);
        });
        parsedData.cache();

        int minNumClusters = 2;
        int maxNumClusters = 20;
        int numIterations = 100;

        StringBuilder stringBuilder = new StringBuilder();
        for(int i = minNumClusters; i <= maxNumClusters; i++) {
            KMeansModel clusters = KMeans.train(parsedData.rdd(), i, numIterations, initModeString(true));
            double cost = clusters.computeCost(parsedData.rdd());
            String str = i + " " + cost + "\n";
            stringBuilder.append(str);
        }
        Files.write(Paths.get(output + "/kMeansClustering.txt"), stringBuilder.toString().getBytes());

        stringBuilder = new StringBuilder();
        for(int i = minNumClusters; i <= maxNumClusters; i++) {
            KMeansModel clusters = KMeans.train(parsedData.rdd(), i, numIterations, initModeString(false));
            double cost = clusters.computeCost(parsedData.rdd());
            String str = i + " " + cost + "\n";
            stringBuilder.append(str);
        }
        Files.write( Paths.get(output + "/randomClustering.txt"), stringBuilder.toString().getBytes());

        jsc.close();

    }

    private static String initModeString(Boolean isKMeans) {
        return isKMeans ? "k-means||" : "random";
    }

}
