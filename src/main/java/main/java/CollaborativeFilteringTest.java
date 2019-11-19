package main.java;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;


/**
 *
 * bin/spark-submit --class main.java.CollaborativeFilteringTest /Users/jakobschwerter/Development/data-mining-praktikum/target/data-mining-praktikum-1.0-SNAPSHOT.jar
 *
 */
public class CollaborativeFilteringTest {

    public static void main(String[] args) {

        CollaborativeFilteringTest cb = new CollaborativeFilteringTest();

        // cb.collaborativeFiltering(2);


        cb.amount();


    }

    public CollaborativeFilteringTest() {

    }

    void amount() {

        SparkConf conf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "/Users/jakobschwerter/Documents/Uni/Moderne Datenbanktechnologien/Praktikum - Data Mining/dataminingpraktikum-master/daten/ratings.txt";

        JavaRDD<String> lines = jsc.textFile(path);

        JavaRDD<Rating> ratings = lines.map(s -> {
            String[] sarray = s.split("\\s+");
            return new Rating(
                    Integer.parseInt(sarray[0]), // user
                    Integer.parseInt(sarray[1]), // movie
                    Double.parseDouble(sarray[2]) // rating
            );
        });

        JavaRDD<Integer> users = ratings.map(r -> r.user());

        System.out.println("Users: " + users.distinct().collect().size());

        JavaRDD<Integer> movies = ratings.map(r -> r.product());

        System.out.println("Movies: " + movies.distinct().collect().size());


        /*
        JavaPairRDD<Integer, Integer> movieratings = ratings.mapToPair(r -> {
            return new Tuple2<Integer, Integer>(r.product(), 1);
        });

        JavaPairRDD<Integer, Integer> movieratings2 = movieratings.reduceByKey((n1, n2) -> n1 + n2);

        movieratings2.sortByKey().collect().forEach(r -> System.out.println(r));
        */

        JavaPairRDD<Integer, Integer> movieratings = ratings.mapToPair(r -> {
            return new Tuple2<Integer, Integer>(r.user(), 1);
        });

        JavaPairRDD<Integer, Integer> movieratings2 = movieratings.reduceByKey((n1, n2) -> n1 + n2);

        movieratings2.sortByKey().collect().forEach(r -> System.out.println(r));


    }


    void collaborativeFiltering(int k) {

        SparkConf conf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);

        Broadcast<Integer> kNum = jsc.broadcast(k);

        String path = "/Users/jakobschwerter/Documents/Uni/Moderne Datenbanktechnologien/Praktikum - Data Mining/dataminingpraktikum-master/daten/ratings.txt";

        String testImputPath = "/Users/jakobschwerter/Documents/Uni/vorlBsp.txt";

        String outputPath = "/Users/jakobschwerter/Documents/Uni/";

        // Load and parse the data
        JavaRDD<String> lines = jsc.textFile(testImputPath);

        /*
        JavaRDD<Vector> rows = lines.map(s -> {
            String[] sarray = s.split("\\s+");
            return Vectors.dense(
                    Integer.parseInt(sarray[0]), // user
                    Integer.parseInt(sarray[1]), // movie
                    Double.parseDouble(sarray[2]) // rating
            );
        });

        JavaRDD<Vector> training = rows.sample(false, 0.8);
        training.cache();
        JavaRDD<Vector> test = rows.subtract(training);
        test.cache();
        */


        JavaRDD<Rating> ratings = lines.map(s -> {
            String[] sarray = s.split("\\s+");
            return new Rating(
                    Integer.parseInt(sarray[0]), // user
                    Integer.parseInt(sarray[1]), // movie
                    Double.parseDouble(sarray[2]) // rating
            );
        });



        CoordinateMatrix ratingCoordinateMatrix = new CoordinateMatrix(ratings.map(r ->
                (new MatrixEntry(r.user(), r.product(), r.rating()))).rdd()
        );

        // ratingCoordinateMatrix.toRowMatrix().columnSimilarities().toRowMatrix().rows().saveAsTextFile(outputPath + "out");




        JavaRDD<Vector> ratingCoordinateMatrixStandardized = ratingCoordinateMatrix.toRowMatrix().rows().toJavaRDD().map(r -> {

            double sum = 0;
            int amount = 0;
            for (double d : r.toDense().toArray()) {
                sum += d;
                if (d != 0) {
                    amount++;
                }
            }

            double avg = sum / amount;

            double[] vec = r.toDense().toArray();

            for (int i = 0; i < vec.length; i++) {
                if (vec[i] != 0) {
                    vec[i] = vec[i] - avg;
                }
            }

            return Vectors.dense(vec);
        });


        CoordinateMatrix cosSimMatrix = ratingCoordinateMatrix.toRowMatrix().columnSimilarities();

        // double[][] cosSimMatrixAccessible = this.getCosSimMatrixAsDouble(cosSimMatrix);

        Broadcast<double[][]> broadcastColSim = jsc.broadcast(this.getCosSimMatrixAsDouble(cosSimMatrix));

        // broadcastColSim.value();


        /*
        for (double[] xyz : cosSimMatrixAccessible) {
            for (double abc : xyz) {
                System.out.print(abc + " ");
            }
            System.out.println();
        }
        System.out.println();
        */




        /*
        double[][] cosinusSimilarities = new double[(int) ratingCoordinateMatrix.numCols() + 1][(int) ratingCoordinateMatrix.numCols() + 1];

        for (int i = 0; i < cosinusSimilarities.length; i++) {
            for (int j = 0; j < cosinusSimilarities[i].length; j++) {

            }
        }
        */


        JavaRDD<Vector> users = ratingCoordinateMatrix.toRowMatrix().rows().toJavaRDD();
        users.cache();

        // JavaRDD<Vector> usersUpdated =



        JavaRDD<Vector> usersNew = users.map(v -> {
            double[] vec = v.toDense().values();
            double[] vecWithNewValues = vec.clone();
            for (int i = 0; i < vec.length; i++) {
                if (vec[i] == 0.0) {
                    int movieID = i;
                    ArrayList<double[]> similiarMovies = new ArrayList<>();
                    for (int j = 0; j < vec.length; j++) {
                        if (i != j && vec[j] != 0) {
                            double colSim = 0.0;

                            colSim = broadcastColSim.value()[i][j];


                            similiarMovies.add(new double[]{vec[j], colSim});
                        }
                    }
                    Collections.sort(similiarMovies, new Comparator<double[]>() {
                        @Override
                        public int compare(double[] d1, double[] d2) {
                            if (d1[1] == d2[1]) {
                                return 0;
                            }
                            return (d1[1] > d2[1]) ? 1 : -1;
                        }
                    });

                    /*
                    System.out.println();
                    for (int wasd = 0; i < similiarMovies.size(); i++) {
                        System.out.println(similiarMovies.get(wasd)[1]);
                    }
                    System.out.println();
                    */

                    double sumCosSimsTimesRating = 0.0;
                    double sumCosSims = 0.0;
                    for (int x = 0; x < kNum.value(); x++) {
                        if (x >= similiarMovies.size()) {
                            break;
                        }
                        sumCosSimsTimesRating += (similiarMovies.get(x)[0] * similiarMovies.get(x)[1]);
                        sumCosSims += similiarMovies.get(x)[1];
                    }

                    if (sumCosSims > 0) {
                        vecWithNewValues[i] = sumCosSimsTimesRating / sumCosSims;
                    } else {
                        vecWithNewValues[i] = 0;
                    }
                }
            }
            for (double d : vecWithNewValues) {
                System.out.print(d + " ");
            }
            System.out.print("|");
            System.out.println();
            return Vectors.dense(vecWithNewValues);
        });

        usersNew.saveAsTextFile(outputPath + "out");



        /*
        for (Vector v : users.collect()) {
            double[] vec = v.toDense().values();
            double[] vecWithNewValues = vec.clone();
            for (int i = 0; i < vec.length; i++) {
                if (vec[i] == 0.0) {
                    int movieID = i;
                    ArrayList<double[]> similiarMovies = new ArrayList<>();
                    for (int j = 0; j < vec.length; j++) {
                        if (i != j && vec[j] != 0) {
                            double colSim = 0.0;

                            colSim = cosSimMatrixAccessible[i][j];


                            similiarMovies.add(new double[]{vec[j], colSim});
                        }
                    }
                    Collections.sort(similiarMovies, new Comparator<double[]>() {
                        @Override
                        public int compare(double[] d1, double[] d2) {
                            if (d1[1] == d2[1]) {
                                return 0;
                            }
                            return (d1[1] > d2[1]) ? 1 : -1;
                        }
                    });
                    */

                    /*
                    System.out.println();
                    for (int wasd = 0; i < similiarMovies.size(); i++) {
                        System.out.println(similiarMovies.get(wasd)[1]);
                    }
                    System.out.println();
                    */

        /*
                    double sumCosSimsTimesRating = 0.0;
                    double sumCosSims = 0.0;
                    for (int x = 0; x < k; x++) {
                        if (x >= similiarMovies.size()) {
                            break;
                        }
                        sumCosSimsTimesRating += (similiarMovies.get(x)[0] * similiarMovies.get(x)[1]);
                        sumCosSims += similiarMovies.get(x)[1];
                    }

                    if (sumCosSims > 0) {
                        vecWithNewValues[i] = sumCosSimsTimesRating / sumCosSims;
                    } else {
                        vecWithNewValues[i] = 0;
                    }
                }
            }
            for (double d : vecWithNewValues) {
                System.out.print(d + " ");
            }
            System.out.print("|");
            System.out.println();
        }
        */



        // JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Collection<>>> mostSimiliarMovies = // movie,










        // JavaPairRDD<Tuple2<Integer, Integer>, Double> trainingResult = null;


        /*

        JavaPairRDD<Integer, Integer> movieRatings = lines.mapToPair(l -> {
            String[] array = l.split("\\s+");
            return new Tuple2<>(Integer.parseInt(array[1]), Integer.parseInt(array[2]));
        });


        JavaPairRDD<Integer, Iterable<Integer>> moviesWithRatings = movieRatings.groupByKey();

        moviesWithRatings.saveAsTextFile(outputPath + "out");

        */





        // RowMatrix rowMat = new RowMatrix(training.rdd());

        // rowMat.rows().saveAsTextFile(outputPath + "testout");


        /*
        JavaRDD<MatrixEntry> entries = training.map(vector ->
                new MatrixEntry((long) vector.toArray()[1], (long) vector.toArray()[0], vector.toArray()[2])
        );
        */
        // CoordinateMatrix coordMat = new CoordinateMatrix(entries.rdd());

        // RowMatrix rowMat = coordMat.toRowMatrix();

        // entries.saveAsTextFile(outputPath + "testout");

        // RowMatrix rowMatTransposed = transposeRM(jsc, rowMat);

        // CoordinateMatrix coords = rowMat.columnSimilarities();


        // coords.toRowMatrix().rows().saveAsTextFile("/Users/jakobschwerter/Documents/Uni/Moderne Datenbanktechnologien/Praktikum - Data Mining/dataminingpraktikum-master/daten/testout");




        /*
        for(Vector v : rows.collect()){
            System.out.println(v);
        }
        */

        jsc.stop();

    }

    private double sumOfArray(double[] arr) {
        double summe = 0;
        for (double d : arr) {
            summe += d;
        }
        return summe;
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

        /*
        for (int i = 0; i < out.length; i++) {
            for (int j = 0; j < out[i].length; j++) {
                System.out.print(out[i][j] + " ");
            }
            System.out.println();
        }

        System.out.println();
        System.out.println();
        */

        return out;

    }


    /**
     * Quelle: https://stackoverflow.com/a/47548453/9606588
     */
    public static RowMatrix transposeRM(JavaSparkContext jsc, RowMatrix mat) {
        List<Vector> newList = new ArrayList<Vector>();
        List<Vector> vs = mat.rows().toJavaRDD().collect();
        double [][] tmp = new double[(int)mat.numCols()][(int)mat.numRows()] ;

        for(int i=0; i < vs.size(); i++){
            double[] rr=vs.get(i).toArray();
            for(int j=0; j < mat.numCols(); j++){
                tmp[j][i]=rr[j];
            }
        }

        for(int i=0; i < mat.numCols();i++)
            newList.add(Vectors.dense(tmp[i]));

        JavaRDD<Vector> rows2 = jsc.parallelize(newList);
        RowMatrix newmat = new RowMatrix(rows2.rdd());
        return (newmat);
    }

    public static RowMatrix mirrorRM(JavaSparkContext jsc, RowMatrix mat) {
        List<Vector> newList = new ArrayList<Vector>();
        List<Vector> vs = mat.rows().toJavaRDD().collect();
        double [][] tmp = new double[(int)mat.numCols()][(int)mat.numRows()] ;

        for(int i=0; i < vs.size(); i++){
            double[] rr=vs.get(i).toArray();
            for(int j=0; j < mat.numCols(); j++){
                tmp[j][i]=rr[j];
            }
        }

        for(int i=0; i < mat.numCols();i++)
            newList.add(Vectors.dense(tmp[i]));

        JavaRDD<Vector> rows2 = jsc.parallelize(newList);
        RowMatrix newmat = new RowMatrix(rows2.rdd());
        return (newmat);
    }



}

