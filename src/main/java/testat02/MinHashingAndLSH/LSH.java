package testat02.MinHashingAndLSH;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;
import com.google.common.hash.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.function.Function;

/**
 * sparkSubmit --class testat02.MinHashingAndLSH.LSH target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class LSH implements java.io.Serializable {

    private String path = "/Users/jakobschwerter/Development/data-mining-praktikum/daten/imdb.txt";

    private SparkConf conf;
    private JavaSparkContext jsc;

    private int k = 3;

    private int numberHashFunctions = 3;

    private static int minHashSignatureSize = 1000;
    private static int primeNumber = 131071;

    public static void main(String[] args) {
        LSH lsh = new LSH();
        lsh.main();
    }

    void main() {
        conf = new SparkConf().set("spark.executor.memory","8G");
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

        JavaPairRDD<Integer, List<Integer>> oneHot = lines.mapToPair(l -> {
            List<Integer> oneHotArr = new ArrayList<>(oneHotReference.value().length);
            for (int i = 0; i < oneHotReference.value().length; i++) {
                if (l.getShingles().contains(oneHotReference.value()[i])) {
                    oneHotArr.add(i, 1);
                } else {
                    oneHotArr.add(i, 0);
                }
            }
            return new Tuple2<Integer, List<Integer>>(l.getID(), oneHotArr);
        });

        HashFunction[] hashFunctions = new HashFunction[numberHashFunctions];
        for (int i = 0; i < hashFunctions.length; i++) {
            hashFunctions[i] = new HashFunction();
        }
        Broadcast<HashFunction[]> hashFunctionsBroadcast = jsc.broadcast(hashFunctions);

        JavaPairRDD<Integer, List<Integer>> permutations = oneHot.mapToPair(m -> {
            List<Integer> list = new ArrayList<>();
            for (HashFunction h : hashFunctionsBroadcast.value()) {
                list.add(h.hash(m._1));
            }
            return new Tuple2<Integer, List<Integer>>(m._1, list);
        });

        Broadcast<List<Tuple2<Integer, List<Integer>>>> permutationsBroadcast = jsc.broadcast(permutations.sortByKey(true).collect());

        JavaPairRDD<Integer, List<Integer>> signatures = oneHot.flatMapToPair(m -> { // (C_x, (h_1_1, h_2_1, ..., h_x_1))
                    List<Tuple2<Tuple2<Integer, Integer>, List<Integer>>> list = new ArrayList<>(); // ((C_x, 0/1), (h1, h2, h3, ...))
                    for (int i = 0; i < m._2.size(); i++) {
                        list.add(
                                new Tuple2<Tuple2<Integer, Integer>, List<Integer>>(
                                        new Tuple2<Integer, Integer>(m._1, m._2.get(i)),
                                        permutationsBroadcast.value().get(m._1 - 1)._2
                                )
                        );
                    }
                    return list.iterator();
                })
                .filter(f -> f._1._2 == 1)
                .mapToPair(m -> new Tuple2<Integer, List<Integer>>(m._1._1, m._2))
                .reduceByKey((n1, n2) -> {
                    List<Integer> list = new ArrayList<>();
                    for (int i = 0; i < n1.size(); i++) {
                        list.add(Math.min(n1.get(i), n2.get(i)));
                    }
                    return list;
                });


        signatures.foreach(s -> System.out.println(s));

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
