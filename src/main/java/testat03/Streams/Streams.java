package testat03.Streams;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * sparkSubmit --class testat03.Streams.Streams target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class Streams {

    private transient SparkConf conf;
    private transient JavaStreamingContext jssc;

    private int topResults = 10;

    public static void main(String[] args) {
        Streams streams = new Streams();
        streams.conf = new SparkConf().set("spark.executor.memory", "8G");
        streams.jssc = new JavaStreamingContext(streams.conf, Durations.seconds(10));
        streams.main(1);
    }

    void main(int variant) {
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        switch (variant) {
            case 1:
                firstVariant(lines);
                break;
            case 2:
                secondVariant(lines);
                break;
            case 3:
                thirdVariant(lines);
                break;
            default:
                return;
        }

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch(Exception e) {

        }
    }

    void firstVariant(JavaReceiverInputDStream<String> lines) {
        JavaPairDStream<Integer, String> wordsWithAmount = lines.mapToPair(m -> new Tuple2<String, Integer>(m, 1))
                .reduceByKey((n1, n2) -> n1 + n2)
                .mapToPair(x -> new Tuple2<Integer, String>(x._2, x._1))
                .transformToPair(rdd -> rdd.sortByKey(false));
        wordsWithAmount.print(topResults);
    }

    void secondVariant(JavaReceiverInputDStream<String> lines) {
        JavaPairDStream<Integer, String> wordsWithAmount = lines.mapToPair(m -> new Tuple2<String, Integer>(m, 1))
                .reduceByKeyAndWindow((n1, n2) -> n1 + n2, Durations.seconds(60), Durations.seconds(10))
                .mapToPair(x -> new Tuple2<Integer, String>(x._2, x._1))
                .transformToPair(rdd -> rdd.sortByKey(false));
        wordsWithAmount.print(topResults);
    }

    void thirdVariant(JavaReceiverInputDStream<String> lines) {
        double c = 0.1;
        double s = 1.0;

    }

}
