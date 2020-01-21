package testat02.MinHashingAndLSH;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

/**
 * sparkSubmit --class testat02.MinHashingAndLSH.LSH target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class LSH implements java.io.Serializable {

    private String path = "/Users/jakobschwerter/Development/data-mining-praktikum/daten/imdb.txt";

    private SparkConf conf;
    private JavaSparkContext jsc;

    private int k = 3;

    private static int minHashSignatureSize = 1000;
    private static int primeNumber = 131071;
    private static int randomNumberLowerBound = 0;
    private static int randomNumberUpperBound = 1000;

    public static void main(String[] args) {
        LSH lsh = new LSH();
        lsh.main();
    }

    void main() {
        conf = new SparkConf();
        jsc = new JavaSparkContext(conf);
        Broadcast<Integer> shingleSize = jsc.broadcast(k);
        JavaRDD<Review> lines = jsc.parallelize(jsc.textFile(path).take(1000)) // jsc.textFile(path) für alle Zeilen
                .map(l -> new Review(l, shingleSize.value()));

        JavaRDD<String> allShingles = lines.flatMapToPair(l -> {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for (String string : l.getShingles()) {
                list.add(new Tuple2<String, Integer>(string, 1));
            }
            return list.iterator();
        }).reduceByKey((n1, n2) -> n1 + n2)
                .filter(f -> f._2 > 1)
                .map(m -> m._1);

        // Referenz-Array mit allen möglichen Shingles
        List<String> shingleList = allShingles.collect();
        String[] tempArray = new String[shingleList.size()];
        tempArray = shingleList.toArray(tempArray);
        Broadcast<String[]> oneHotReference = jsc.broadcast(tempArray);

        JavaPairRDD<Integer, int[]> oneHot = lines.mapToPair(l -> {
            int[] oneHotArr = new int[oneHotReference.value().length];
            for (int i = 0; i < oneHotReference.value().length; i++) {
                if (l.getShingles().contains(oneHotReference.value()[i])) {
                    oneHotArr[i] = 1;
                } else {
                    oneHotArr[i] = 0;
                }
            }
            return new Tuple2<Integer, int[]>(l.getID(), oneHotArr);
        });




    }

    static int h(int x) {
        // (( a * x + b) mod p) mod N
        return ((((randomNumber() * x) + randomNumber()) % primeNumber) % minHashSignatureSize);
    }

    static int randomNumber() {
        return (int) ((Math.random() * (randomNumberUpperBound - randomNumberLowerBound)) + randomNumberLowerBound);
    }

    static String arrayAsString(int[] arr) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        boolean begin = true;
        for (int i : arr) {
            stringBuilder.append(i);
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

}
