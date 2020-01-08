package testat01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * sparkSubmit --class testat01.CollaborativeFiltering target/data-mining-praktikum-1.0-SNAPSHOT.jar
 *
 * Nutzer: 943
 * Filme: 1682
 */
public class CollaborativeFiltering {

    private JavaSparkContext jsc;

    private String path = "daten/ratings.txt";

    CollaborativeFiltering() {
        SparkConf conf = new SparkConf();
        jsc = new JavaSparkContext(conf);
    }

    public static void main(String[] args) throws Exception {

        CollaborativeFiltering cf = new CollaborativeFiltering();

        String output = "output/testat01/CollaborativeFiltering";
        StringBuilder stringBuilder = new StringBuilder();
        for (int k = 5; k <= 50; k = k + 5) {
            double rmse = cf.collaborativeFiltering(cf.path, k);
            System.out.println("[k=" + k + "] RMSE: " + rmse);
            stringBuilder.append(k + " " + rmse + "\n");
        }
        Files.write( Paths.get(output + "/collaborativeFiltering.txt"), stringBuilder.toString().getBytes());

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
        JavaRDD<Rating> test = ratings.subtract(training);

        IndexedRowMatrix ratingIndexedRowMatrix = new CoordinateMatrix(training.map(r ->
                (new MatrixEntry(r.user(), r.product(), r.rating()))).rdd()
        ).toIndexedRowMatrix();

        CoordinateMatrix cosSimMatrix = ratingIndexedRowMatrix.columnSimilarities();

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

        // JavaPairRDD<Tuple2<NutzerID, FilmID>, Tuple<(Ähnlichkeit * Rating), Ähnlichkeit>>
        JavaPairRDD<Tuple2<Long, Integer>, Tuple2<Double, Double>> userRatingsWithSimilarities = ratingIndexedRowMatrix.rows().toJavaRDD().flatMapToPair(r -> {

            // r: index (NutzerID) & vector (Bewertungen des Nutzers)

            // <Tuple2<Tuple2<NutzerID, FilmID x>, Tuple<(Ähnlichkeit * Rating), Ähnlichkeit>>
            ArrayList<Tuple2<Tuple2<Long, Integer>, Tuple2<Double, Double>>> list = new ArrayList<>();

            double[] userRatingsArray = r.vector().toArray(); // alle Bewertungen des Nutzers

            for (int i = 0; i < broadcastColSim.value().length; i++) { // Anzahl aller Filme

                int movieID = i;

                double[] ratingSimilarities = broadcastColSim.value()[movieID]; // Ähnlichkeiten aller Filme für Film 'movieID'

                int amountOfMovies = ratingSimilarities.length;

                ArrayList<Tuple2<Tuple2<Integer, Double>, Double>> movieRatingsWithSimilarities = new ArrayList<>();

                for (int j = 0; j < amountOfMovies; j++) {

                    if (j < userRatingsArray.length && userRatingsArray[j] != 0 && j != movieID) {

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
                                        j, // ID des Films, dessen Änhlichkeit zum aktuellen Film betrachtet wird
                                        r.vector().apply(j) // Bewertung des Nutzers zu dem Film
                                ),
                                ratingSimilarities[j] // Ähnlichkeit des Films zu dem betrachteten Film
                        ));

                    }

                }

                for (Tuple2<Tuple2<Integer, Double>, Double> t : movieRatingsWithSimilarities) {
                    list.add(new Tuple2<Tuple2<Long, Integer>, Tuple2<Double, Double>>(
                            new Tuple2<Long, Integer>(r.index(), movieID), // userID, filmID
                            new Tuple2<Double, Double>((t._1._2 * t._2), t._2) // ('hnlichkeit * rating), ähnlichkeit
                    ));
                }

            }

            return list.iterator();

        });

        // JavaPairRDD<Tuple2<NutzerID, FilmID>, Tuple<(Ähnlichkeit * Rating), Ähnlichkeit>> -> gruppiert nach key
        JavaPairRDD<Tuple2<Long, Integer>, Tuple2<Double, Double>> userRatingsWithSums = userRatingsWithSimilarities.reduceByKey((n1, n2) -> {
            return new Tuple2<Double, Double>( (n1._1 + n2._1), (n1._2 + n2._2) );
        });

        // JavaPairRDD<Tuple2<NutzerID, FilmID>, Prediction>
        JavaPairRDD<Tuple2<Long, Integer>, Double> userPredictions = userRatingsWithSums.filter(x -> {
            return (x._2._2 != 0); // filter: Filme, für die keine Prediction bestimmt werden konnte, weil alle Ähnlickeiten 0 sind
        }).mapToPair(t -> new Tuple2<Tuple2<Long, Integer>, Double>(t._1, (t._2._1 / t._2._2)));

        /*
         * Berechnung der Bewertungsmetrik:
         */

        JavaPairRDD<Tuple2<Long, Integer>, Double> userRatings = test.mapToPair(r ->
            new Tuple2<Tuple2<Long, Integer>, Double>(new Tuple2<Long, Integer>((long)r.user(), r.product()), r.rating())
        );

        JavaPairRDD<Object, Object> ratesAndPreds = userRatings.join(userPredictions).values().mapToPair(val ->
                new Tuple2<>(val._1(), val._2())
        );

        RegressionMetrics metrics = new RegressionMetrics(ratesAndPreds.rdd());
        double rmse = metrics.rootMeanSquaredError();

        return rmse;

    }

    /**
     * Cos-Ähnlichkeitsmatrix als double[][].
     * Dreiecksmatrix wird dabei gespiegelt.
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
