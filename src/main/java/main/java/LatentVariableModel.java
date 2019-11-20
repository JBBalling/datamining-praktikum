package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

/**
 * bin/spark-submit --class main.java.LatentVariableModel /Users/jakobschwerter/Development/data-mining-praktikum/target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class LatentVariableModel {

    public static void main(String[] args) {

        als(5, 0.01, 10);

    }

    static Double als(int rank, double lambda, int iterations) {

        SparkConf conf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "/Users/jakobschwerter/Documents/Uni/Moderne Datenbanktechnologien/Praktikum - Data Mining/dataminingpraktikum-master/daten/ratings.txt";

        // Load and parse the data
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Rating> ratings = data.map(s -> {
            String[] sarray = s.split("\\s+");
            return new Rating(Integer.parseInt(sarray[0]),
                    Integer.parseInt(sarray[1]),
                    Double.parseDouble(sarray[2]));
        });

        JavaRDD<Rating> training = ratings.sample(false, 0.8);
        training.cache();
        JavaRDD<Rating> test = ratings.subtract(training);
        test.cache();

        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(training), rank, iterations, lambda);

        // Evaluate the model on test data: create relation U - P
        JavaRDD<Tuple2<Object, Object>> userProducts =
                test.map(r -> new Tuple2<>(r.user(), r.product()));

        // predict on relation U - P
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
                        .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
        );

        JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(
                test.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
                .join(predictions).values();

        JavaPairRDD<Object, Object> ratesAndPreds2 = ratesAndPreds.mapToPair(val ->
                new Tuple2<>(val._1, val._2)
        );

        RegressionMetrics metrics = new RegressionMetrics(ratesAndPreds2.rdd());

        double rmse = metrics.rootMeanSquaredError();

        System.out.println("RMSE = " + rmse);

        jsc.close();

        return rmse;

    }

}
