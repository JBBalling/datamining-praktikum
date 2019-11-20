package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * bin/spark-submit --class main.java.CollaborativeFiltering /Users/jakobschwerter/Development/data-mining-praktikum/target/data-mining-praktikum-1.0-SNAPSHOT.jar
 *
 * Nutzer: 943
 * Filme: 1682
 */
public class CollaborativeFiltering {

    private JavaSparkContext jsc;

    private String path = "/Users/jakobschwerter/Documents/Uni/Moderne Datenbanktechnologien/Praktikum - Data Mining/dataminingpraktikum-master/daten/ratings.txt";

    CollaborativeFiltering() {
        SparkConf conf = new SparkConf();
        jsc = new JavaSparkContext(conf);
    }

    public static void main(String[] args) {

        CollaborativeFiltering cf = new CollaborativeFiltering();

        for (int i = 5; i <= 30; i = i + 5) {
            System.out.println("[k=" + i + "] RMSE: " + cf.collaborativeFiltering(cf.path, i));
        }

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

        JavaRDD<Rating> training = ratings.sample(false, 0.8, 11L);
        training.cache();
        JavaRDD<Rating> test = ratings.subtract(training);
        test.cache();

        // standardisieren?

        CoordinateMatrix ratingCoordinateMatrixTraining = new CoordinateMatrix(training.map(r -> // training
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

        // JavaPairRDD<Tuple2<NutzerID, FilmID x>, ArrayList<Tuple2<Tuple2<FilmID y, Bewertung>, Cos-Ähnlichkeit von Film x zu y>>>
        JavaPairRDD<Tuple2<Long, Integer>, ArrayList<Tuple2<Tuple2<Integer, Double>, Double>>> userRatingsWithSimilarities = ratingCoordinateMatrixTraining.toIndexedRowMatrix().rows().toJavaRDD().flatMapToPair(r -> {

            // r: index (NutzerID) & vector (Bewertungen des Nutzers)

            // <Tuple2<Tuple2<NutzerID, FilmID x>, ArrayList<Tuple2<Tuple2<FilmID y, Nutzerrating>, Ähnlichkeit x zu y>>>
            ArrayList<Tuple2<Tuple2<Long, Integer>, ArrayList<Tuple2<Tuple2<Integer, Double>, Double>>>> list = new ArrayList<>();

            double[] userRatingsArray = r.vector().toArray(); // alle Bewertungen des Nutzers

            for (int i = 0; i < broadcastColSim.value().length; i++) { // Anzahl aller Filme

                int movieID = i;

                double[] ratingSimilarities = broadcastColSim.value()[movieID]; // Ähnlichkeiten aller Filme für Film 'movieID'

                int amountOfMovies = ratingSimilarities.length;

                ArrayList<Tuple2<Tuple2<Integer, Double>, Double>> movieRatingsWithSimilarities = new ArrayList<>();

                for (int j = 0; j < amountOfMovies; j++) {

                    if (j < userRatingsArray.length && userRatingsArray[j] != 0 && j != movieID) {

                        /*
                        optimieren: liste immer sortiert halten. wert an passender stelle einfügen und index 0 entfernen
                         */
                        if (movieRatingsWithSimilarities.size() == k.value()) {
                            /*
                            Es sind bereits k ähnlichste Filme enthalten, also wird der Film mit der niedrigsten Ähnlichkeit entfernt
                             */

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
                                        j, // ID des Films
                                        r.vector().apply(j) // Bewertung des Nutzers zu dem Film
                                ),
                                ratingSimilarities[j] // Ähnlichkeit des Films zu dem betrachteten Film
                        ));

                    }

                }

                list.add(new Tuple2<Tuple2<Long, Integer>, ArrayList<Tuple2<Tuple2<Integer, Double>, Double>>>(
                        new Tuple2<Long, Integer>(r.index(), movieID), // (NutzerID, FilmID)
                        movieRatingsWithSimilarities // ArrayList mit k ähnlichsten Filmen die vom Nutzer bewertet wurden
                ));

            }

            return list.iterator();

        });

        // JavaPairRDD<Tuple2<Tuple2<NutzerID, FilmID>, ArrayList<Tuple2<Tuple2<FilmID, Bewertung>, Ähnlichkeit>>>, Prediction>
        JavaPairRDD<Tuple2<Tuple2<Long, Integer>, ArrayList<Tuple2<Tuple2<Integer, Double>, Double>>>, Double> userRatingsWithPredictions = userRatingsWithSimilarities.mapToPair(p -> {

            double sumCosSimilarititesTimesRating = 0.0; // Summe(Ähnlichkeite * Bewertungen)
            double sumCosSimilarities = 0.0; // Summe(Ähnlichkeiten)

            for (Tuple2<Tuple2<Integer, Double>, Double> t : p._2()) {
                sumCosSimilarititesTimesRating += (t._2() * t._1()._2());
                sumCosSimilarities += t._2;
            }

            if (sumCosSimilarities == 0) {
                // System.out.println("0: " + p);
                return new Tuple2<>(p, 0.0); // es konnte keine Schätzung berechnet werden
            }

            double prediction = sumCosSimilarititesTimesRating / sumCosSimilarities; // geschätzte Bewertung

            return new Tuple2<>(p, prediction);

        });

        /*
         * Berechnung der Bewertungsmetrik:
         */

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

    /**
     *
     * Cos-Ähnlichkeitsmatrix als double[][].
     * Dreiecksmatrix wird dabei gespiegelt.
     *
     */
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
