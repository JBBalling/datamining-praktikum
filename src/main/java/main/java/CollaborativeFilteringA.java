package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

public class CollaborativeFilteringA {

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

        JavaRDD<Rating> training = ratings.sample(false, 0.8, 11L);
        training.cache();
        JavaRDD<Rating> test = ratings.subtract(training);

        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(training), rank, iterations, lambda);

        JavaRDD<Tuple2<Object, Object>> userProducts = test.map(r -> new Tuple2<>(r.user(), r.product()));

        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
                        .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
        );

        JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(
                test.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))).join(predictions).values();

        double MSE = ratesAndPreds.mapToDouble(pair -> {
            double err = pair._1() - pair._2();
            return err * err;
        }).mean();

        double RMSE = Math.sqrt(MSE);

        System.out.println("Root Mean Squared Error = " + RMSE);

        // Save and load model
        // model.save(jsc.sc(), "target/tmp/myCollaborativeFilter");
        // MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(),"target/tmp/myCollaborativeFilter");

        jsc.close();

        return RMSE;
    }

}
