package testat03.Streams;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import org.apache.spark.api.java.Optional;

import java.util.List;

/**
 * sparkSubmit --class testat03.Streams.Streams target/data-mining-praktikum-1.0-SNAPSHOT.jar [1/2/3]
 */
public class Streams {

    private transient SparkConf conf;
    private transient JavaStreamingContext jssc;

    private int topResults = 10;

    public static void main(String[] args) {
        Streams streams = new Streams();
        streams.conf = new SparkConf().set("spark.executor.memory", "8G");
        streams.jssc = new JavaStreamingContext(streams.conf, Durations.seconds(10));
        streams.jssc.checkpoint("checkpoints");
        streams.main(Integer.parseInt(args[0]));
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
            e.printStackTrace();
        }
    }

    // erste Variante (alle 10s absolute Häufigkeiten aller Wörter bestimmen):
    void firstVariant(JavaReceiverInputDStream<String> lines) {
        JavaPairDStream<Integer, String> wordsWithAmount = lines.mapToPair(m -> new Tuple2<String, Integer>(m, 1))
                .reduceByKey((n1, n2) -> n1 + n2)
                .mapToPair(x -> new Tuple2<Integer, String>(x._2, x._1))
                .transformToPair(rdd -> rdd.sortByKey(false));
        wordsWithAmount.print(topResults);
    }

    // zweite Variante (absolute Häufigkeit aller Wörter der letzten 60s, aber Berechnung alle 10s):
    void secondVariant(JavaReceiverInputDStream<String> lines) {
        JavaPairDStream<Integer, String> wordsWithAmount = lines.mapToPair(m -> new Tuple2<String, Integer>(m, 1))
                .reduceByKeyAndWindow((n1, n2) -> n1 + n2, Durations.seconds(60), Durations.seconds(10)) // letzte x Sekunden, alle y Sekunden
                .mapToPair(x -> new Tuple2<Integer, String>(x._2, x._1))
                .transformToPair(rdd -> rdd.sortByKey(false));
        wordsWithAmount.print(topResults);
    }

    // dritte Variante (Exponentially Decaying Window):
    void thirdVariant(JavaReceiverInputDStream<String> lines) {
        double c = 0.1;
        double s = 1.0;
        Function2<List<Double>, Optional<Double>, Optional<Double>> updateFunction =
                (values, state) -> { // (neue Werte, alter Zustand)
                    Double SX = values.size() + ((1 - c) * state.orElse(0.0)); // neue Werte (nur Einsen: Size = Summe) + (1 - c) * alter Zustand
                    if (SX < s) {
                        return Optional.empty(); // Zähler entfernen
                    }
                    return Optional.of(SX);
                };
        JavaPairDStream<String, Double> words = lines.mapToPair(m -> new Tuple2<String, Double>(m, 1.0)); // Wörter-Ströme
        JavaPairDStream<Double, String> counts = words.updateStateByKey(updateFunction)
                .mapToPair(m -> new Tuple2<Double, String>(m._2, m._1))
                .transformToPair(rdd -> rdd.sortByKey(false));
        counts.print(topResults);
        // runningCounts.count().print();
    }

}
