package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import shapeless.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * bin/spark-submit --class main.java.CollaborativeFiltering /Users/jakobschwerter/Development/data-mining-praktikum/target/data-mining-praktikum-1.0-SNAPSHOT.jar
 *
 * Nutzer: 943
 * Filme: 1682
 */
public class CollaborativeFiltering {

    private JavaSparkContext jsc;

    private String path = "/Users/jakobschwerter/Documents/Uni/Moderne Datenbanktechnologien/Praktikum - Data Mining/dataminingpraktikum-master/daten/ratings.txt";

    private String testImputPath = "/Users/jakobschwerter/Documents/Uni/vorlBsp.txt";

    private String outputPath = "/Users/jakobschwerter/Documents/Uni/";

    CollaborativeFiltering() {
        SparkConf conf = new SparkConf();
        jsc = new JavaSparkContext(conf);
    }

    public static void main(String[] args) {

        CollaborativeFiltering cf = new CollaborativeFiltering();

        System.out.println("[k=5] RMSE: " + cf.collaborativeFiltering(cf.path, 5));

        System.out.println("[k=10] RMSE: " + cf.collaborativeFiltering(cf.path, 10));

        System.out.println("[k=15] RMSE: " + cf.collaborativeFiltering(cf.path, 15));

        System.out.println("[k=20] RMSE: " + cf.collaborativeFiltering(cf.path, 20));

        System.out.println("[k=25] RMSE: " + cf.collaborativeFiltering(cf.path, 25));

        System.out.println("[k=30] RMSE: " + cf.collaborativeFiltering(cf.path, 30));

        cf.jsc.stop();

    }

    private double collaborativeFiltering(String filePath, int kAmount) {

        Broadcast<Integer> k = jsc.broadcast(kAmount);

        JavaRDD<String> lines = jsc.textFile(filePath);

        JavaRDD<Rating> ratings = lines.map(s -> {
            String[] sarray = s.split("\\s+");
            return new Rating(
                    Integer.parseInt(sarray[0]) - 1, // user (0...) -> ab 0
                    Integer.parseInt(sarray[1]) - 1, // movie (0...) -> ab 0
                    Double.parseDouble(sarray[2]) // rating (1-5)
            );
        });

        JavaRDD<Rating> training = ratings.sample(false, 0.8);
        training.cache();
        JavaRDD<Rating> test = ratings.subtract(training);
        test.cache();


        // standardisieren?


        CoordinateMatrix ratingCoordinateMatrixTraining = new CoordinateMatrix(training.map(r -> // training
                (new MatrixEntry(r.user(), r.product(), r.rating()))).rdd()
        );

        CoordinateMatrix ratingCoordinateMatrixTest = new CoordinateMatrix(test.map(r -> // test
                (new MatrixEntry(r.user(), r.product(), r.rating()))).rdd()
        );

        CoordinateMatrix cosSimMatrix = ratingCoordinateMatrixTraining.toIndexedRowMatrix().columnSimilarities();

        Broadcast<double[][]> broadcastColSim = jsc.broadcast(getCosSimMatrixAsDouble(cosSimMatrix)); // broadcastColSim.value() zum abrufen

        /*
        for (double[] xyz : broadcastColSim.value()) {
            for (double abc : xyz) {
                System.out.print(abc + " ");
            }
            System.out.println();
        }
        System.out.println();
        */

        JavaPairRDD<Tuple2<Long, Integer>, ArrayList<Tuple2<Tuple2<Integer, Double>, Double>>> userRatingsWithSimilarities = ratingCoordinateMatrixTraining.toIndexedRowMatrix().rows().toJavaRDD().flatMapToPair(r -> {

            ArrayList<Tuple2<Tuple2<Long, Integer>, ArrayList<Tuple2<Tuple2<Integer, Double>, Double>>>> list = new ArrayList<>();

            double[] userRatingsArray = r.vector().toArray();

            for (int i = 0; i < broadcastColSim.value().length; i++) {
            // for (int movieID : r.vector().toSparse().indices()) {

                int movieID = i;

                if (movieID >= broadcastColSim.value().length) {
                    continue;
                }

                double[] ratingSimilarities = broadcastColSim.value()[movieID]; // für Film movieID

                int amountOfMovies = ratingSimilarities.length;

                ArrayList<Tuple2<Tuple2<Integer, Double>, Double>> movieRatingsWithSimilarities = new ArrayList<>();

                for (int j = 0; j < amountOfMovies; j++) {

                    if (j < userRatingsArray.length && userRatingsArray[j] != 0 && j != movieID) {

                        if (movieRatingsWithSimilarities.size() == k.value()) {
                            double smallest = Double.MAX_VALUE;
                            int indexSmallest = -1;
                            for (int x = 0; x < movieRatingsWithSimilarities.size(); x++) {
                                if (movieRatingsWithSimilarities.get(x)._2() < smallest) {
                                    smallest = movieRatingsWithSimilarities.get(x)._2();
                                    indexSmallest = x;
                                }
                            }

                            if (smallest > ratingSimilarities[j]) {
                                continue;
                            }

                            if (indexSmallest != -1) {
                                movieRatingsWithSimilarities.remove(indexSmallest);
                            }
                        }

                        movieRatingsWithSimilarities.add(new Tuple2<Tuple2<Integer, Double>, Double>(
                                new Tuple2<Integer, Double>(
                                        j,
                                        r.vector().apply(j)
                                ),
                                ratingSimilarities[j]
                        ));

                    }

                }

                list.add(new Tuple2<Tuple2<Long, Integer>, ArrayList<Tuple2<Tuple2<Integer, Double>, Double>>>(
                        new Tuple2<Long, Integer>(r.index(), movieID),
                        movieRatingsWithSimilarities
                ));

            }

            return list.iterator();

        });

        JavaPairRDD<Tuple2<Tuple2<Long, Integer>, ArrayList<Tuple2<Tuple2<Integer, Double>, Double>>>, Double> userRatingsWithPredictions = userRatingsWithSimilarities.mapToPair(p -> {

            double sumCosSimilarititesTimesRating = 0.0;
            double sumCosSimilarities = 0.0;

            for (Tuple2<Tuple2<Integer, Double>, Double> t : p._2()) {
                sumCosSimilarititesTimesRating += (t._2() * t._1()._2());
                sumCosSimilarities += t._2;
            }

            if (sumCosSimilarities == 0) {
                // System.out.println("0: " + p);
                return new Tuple2<>(p, 0.0); // !!!!!!!!!!!!
            }

            double prediction = sumCosSimilarititesTimesRating / sumCosSimilarities;

            return new Tuple2<>(p, prediction);

        });

        JavaPairRDD<Tuple2<Long, Integer>, Double> userRatings = test.mapToPair(r ->
            new Tuple2<Tuple2<Long, Integer>, Double>(new Tuple2<Long, Integer>((long)r.user(), r.product()), r.rating())
        );

        JavaPairRDD<Tuple2<Long, Integer>, Double> userPredictions = userRatingsWithPredictions.mapToPair(val ->
                new Tuple2<Tuple2<Long, Integer>, Double>(val._1()._1(), val._2())
        ).filter(x -> {
            return (x._2() != 0);
        });

        JavaPairRDD<Object, Object> ratesAndPreds = userRatings.join(userPredictions).values().mapToPair(val ->
                new Tuple2<>(val._1(), val._2())
        );

        // ratesAndPreds.foreach(e -> System.out.println(e));

        RegressionMetrics metrics = new RegressionMetrics(ratesAndPreds.rdd());
        double rmse = metrics.rootMeanSquaredError();

        return rmse;

    }

    private double[][] getCosSimMatrixAsDouble(CoordinateMatrix cosSimMat) {

        double[][] out = new double[(int)cosSimMat.numCols()][(int)cosSimMat.numRows()];

        JavaRDD<MatrixEntry> meRDD = cosSimMat.entries().toJavaRDD();

        for (MatrixEntry me : meRDD.collect()) {
            out[(int) me.i()][(int) me.j()] = me.value();
            out[(int) me.j()][(int) me.i()] = me.value();
        }

        for (int i = 0; i < (int)cosSimMat.numCols(); i++) {
            out[i][i] = 1.0;
        }

        return out;

    }

}
