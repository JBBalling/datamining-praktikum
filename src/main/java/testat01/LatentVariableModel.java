package testat01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * sparkSubmit --class testat01.LatentVariableModel target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class LatentVariableModel {

    public static void main(String[] args) throws Exception{

        String output = "output/testat01/LatentVariableModel";

        int[] ranks = new int[]{5, 10};
        double[] lambdas = new double[]{0.01, 0.1, 1};
        int iterations = 10;

        StringBuilder stringBuilder;

        for (int rank : ranks) { // ranks
            for (double lambda : lambdas) { // lambdas
                stringBuilder = new StringBuilder();
                for (int i = 2; i <= iterations; i++) { // iterations
                    stringBuilder.append(i + " " + als(rank, lambda, i) + "\n");
                }
                String filename = rank + "_" + lambda;
                Files.write(Paths.get(output + "/" + filename + ".txt"), stringBuilder.toString().getBytes());
            }
        }

    }

    static Double als(int rank, double lambda, int iterations) {

        SparkConf conf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "daten/ratings.txt";

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

        System.out.println("RMSE(rank: " + rank + ", lambda: " +  lambda + ", iterations: " + iterations + ") = " + rmse);

        jsc.close();

        return rmse;

    }

}
