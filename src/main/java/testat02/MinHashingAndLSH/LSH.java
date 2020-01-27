package testat02.MinHashingAndLSH;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import scala.Tuple2;
import com.google.common.hash.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * sparkSubmit --class testat02.MinHashingAndLSH.LSH target/data-mining-praktikum-1.0-SNAPSHOT.jar
 *
 * sparkSubmit --conf "spark.driver.extraJavaOptions=-Xms4g" --executor-memory 4g --driver-memory 4g --class testat02.MinHashingAndLSH.LSH target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class LSH implements java.io.Serializable {

    private SparkConf conf;
    private JavaSparkContext jsc;

    static int numberHashFunctions = 1000;
    private int k = 3;
    private int bands = 150;
    private int rows = numberHashFunctions / bands;
    private double minSimilarity = 0.8;

    private String path = "/Users/jakobschwerter/Development/data-mining-praktikum/daten/imdb.txt";

    public static void main(String[] args) {
        LSH lsh = new LSH();
        lsh.main();
        lsh.jsc.stop();
    }

    static double jaccardSimilarity(List<Integer> list1, List<Integer> list2) throws Exception {
        if (list1.size() != list2.size()) {
            throw new Exception();
        }
        int same = 0;
        int allWithOne = 0;
        for (int i = 0; i < list1.size(); i++) {
            if (list1.get(i) == 1 && list2.get(i) == 1) {
                same++;
            }
            if (list1.get(i) == 1 || list2.get(i) == 1) {
                allWithOne++;
            }
        }
        return (double) same / (double) allWithOne;
    }

    static double jaccardSimilarity2(List<Integer> list1, List<Integer> list2) throws Exception {
        if (list1.size() != list2.size()) {
            throw new Exception();
        }
        int same = 0;
        for (int i = 0; i < list1.size(); i++) {
            if (list1.get(i).equals(list2.get(i))) {
                same++;
            }
        }
        return (double) same / (double) list1.size();
    }

    void main() {

        conf = new SparkConf().set("spark.executor.memory", "8G");
        jsc = new JavaSparkContext(conf);
        Broadcast<Integer> shingleSize = jsc.broadcast(k);
        Broadcast<Integer> bandsBroadcast = jsc.broadcast(bands);
        Broadcast<Integer> rowsBroadcast = jsc.broadcast(rows);
        Broadcast<Double> minSimilarityBroadcast = jsc.broadcast(minSimilarity);

        // Daten einlesen
        JavaRDD<Review> lines = jsc.parallelize(jsc.textFile(path).take(10000)) // jsc.textFile(path) für alle Zeilen
                .map(l -> new Review(l, shingleSize.value()));

        // alle Shingles aus allen Dokumenten
        JavaRDD<String> allShingles = lines.flatMapToPair(l -> {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for (String string : l.getShingles()) {
                list.add(new Tuple2<String, Integer>(string, 1));
            }
            return list.iterator();
        }).reduceByKey((n1, n2) -> n1 + n2) // nur Shingles, die in mindestens 2 Dokumenten vorkommen
                .filter(f -> f._2 > 1)
                .map(m -> m._1);

        // Referenz-Array mit allen möglichen Shingles
        List<String> shingleList = allShingles.collect();
        String[] tempArray = new String[shingleList.size()];
        tempArray = shingleList.toArray(tempArray);
        Broadcast<String[]> oneHotReference = jsc.broadcast(tempArray);

        // one-hot Kodierung
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

        // Hashfunktionen für minHash
        HashFunction[] hashFunctions = new HashFunction[numberHashFunctions];
        for (int i = 0; i < hashFunctions.length; i++) {
            hashFunctions[i] = new HashFunction();
        }
        Broadcast<HashFunction[]> hashFunctionsBroadcast = jsc.broadcast(hashFunctions);

        // Permutationen
        List<List<Integer>> permutations = new ArrayList<List<Integer>>(); // jede List sind die Hashes einer Zeilennummer
        for (int i = 0; i < oneHotReference.value().length; i++) {
            List<Integer> innerList = new ArrayList<>();
            for (HashFunction h : hashFunctions) {
                innerList.add(h.hash(i));
            }
            permutations.add(innerList);
        }
        Broadcast<List<List<Integer>>> permutationsBroadcast = jsc.broadcast(permutations);

        // Signaturen
        JavaPairRDD<Integer, List<Integer>> signatures = oneHot.mapToPair(m -> { // (documentId, (signature))
            List<Integer> sig = new ArrayList<>();
            for (int i = 0; i < hashFunctionsBroadcast.value().length; i++) {
                sig.add(Integer.MAX_VALUE);
            }
            for (int i = 0; i < m._2.size(); i++) {
                if (m._2.get(i) == 1) {
                    for (int j = 0; j < permutationsBroadcast.value().get(i).size(); j++) {
                        if (permutationsBroadcast.value().get(i).get(j) < sig.get(j)) {
                            sig.set(j, permutationsBroadcast.value().get(i).get(j));
                        }
                    }
                }
            }
            return new Tuple2<Integer, List<Integer>>(m._1, sig);
        });

        // Auf Bänder aufteilen und hashen
        JavaPairRDD<Tuple2<Integer, HashCode>, Integer> signaturesDividedInBands = signatures.flatMapToPair(f -> {
            List<Tuple2<Tuple2<Integer, HashCode>, Integer>> list = new ArrayList<>();
            int band = 1;
            for (int i = 0; i < (bandsBroadcast.value() * rowsBroadcast.value()); i = i + rowsBroadcast.value()) {
                List<Integer> rowList = f._2.subList(i, i + rowsBroadcast.value());
                com.google.common.hash.HashFunction hashFunction = Hashing.murmur3_32(band);
                Hasher hasher = hashFunction.newHasher();
                for (Integer integ : rowList) {
                    hasher.putInt(integ);
                }
                list.add(new Tuple2<Tuple2<Integer, HashCode>, Integer>(new Tuple2<Integer, HashCode>(band, hasher.hash()), f._1));
                band++;
            }
            return list.iterator();
        });

        // mögliche Paare aus den Listen bilden
        JavaPairRDD<Integer, Integer> similiarDocuments = signaturesDividedInBands.groupByKey() // alle wahrscheinlichen Paare als Tupel
                .map(m -> Lists.newArrayList(m._2))
                .filter(f -> f.size() > 1)
                .flatMapToPair(p -> {
                    List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                    for (int i = 0; i < p.size(); i++) {
                        for (int j = i + 1; j < p.size(); j++) {
                            Tuple2<Integer, Integer> tuple = new Tuple2<>(p.get(i), p.get(j));
                            list.add(tuple);
                        }
                    }
                    return list.iterator();
                }).distinct();

        JavaPairRDD<Integer, Tuple2<List<Integer>, List<Integer>>> oneHotAndSignatures = oneHot.join(signatures); // (id, (oneHot-List, signature-List))

        // Paare mit zugehörigen OneHot-Kodierungen und Signaturen
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Tuple2<List<Integer>, List<Integer>>, Tuple2<List<Integer>, List<Integer>>>> pairsOneHotAndSignatures = similiarDocuments.flatMapToPair(f -> { // ((a, b), ((oneHotA, SigA), (oneHotB, SigB)))
            List<Tuple2<Integer, Tuple2<Integer, Integer>>> list = new ArrayList<>();
            list.add(new Tuple2<Integer, Tuple2<Integer, Integer>>(f._1, f));
            list.add(new Tuple2<Integer, Tuple2<Integer, Integer>>(f._2, f));
            return list.iterator();
        })
                .join(oneHotAndSignatures)
                .mapToPair(m -> {
                    Tuple2<Integer, Integer> pair = m._2._1;
                    Integer documentID = m._1;
                    Tuple2<List<Integer>, List<Integer>> lists = m._2._2;
                    return new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Tuple2<List<Integer>, List<Integer>>>>(pair, new Tuple2<Integer, Tuple2<List<Integer>, List<Integer>>>(documentID, lists));
                })
                .groupByKey()
                .mapToPair(p -> {
                    Tuple2<Integer, Integer> pair = p._1;
                    Iterator<Tuple2<Integer, Tuple2<List<Integer>, List<Integer>>>> iterator = p._2.iterator();
                    Tuple2<Integer, Tuple2<List<Integer>, List<Integer>>> first = iterator.next();
                    Tuple2<Integer, Tuple2<List<Integer>, List<Integer>>> second = iterator.next();
                    Tuple2<List<Integer>, List<Integer>> firstLists = (pair._1 == first._1) ? first._2 : second._2;
                    Tuple2<List<Integer>, List<Integer>> secondLists = (pair._2 == second._1) ? second._2 : first._2;
                    return new Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<List<Integer>, List<Integer>>, Tuple2<List<Integer>, List<Integer>>>>(pair, new Tuple2<Tuple2<List<Integer>, List<Integer>>, Tuple2<List<Integer>, List<Integer>>>(firstLists, secondLists));
                });

        // Jaccard- und MinHash-Ähnlichkeiten berechnen
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> pairsWithBothSimilarities = pairsOneHotAndSignatures.mapToPair(p -> {
            double jaccard = jaccardSimilarity(p._2._1._1, p._2._2._1);
            double minHash = jaccardSimilarity2(p._2._1._2, p._2._2._2);
            return new Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>(p._1, new Tuple2<Double, Double>(jaccard, minHash));
        }).filter(x -> x._2._2 >= minSimilarityBroadcast.value()); // filtern nach Mindest-MinHash-Ähnlichkeit

        pairsWithBothSimilarities.foreach(s -> System.out.println(s));
        System.out.println("Amount of candidates: " + pairsWithBothSimilarities.count());

        JavaPairRDD<Object, Object> rmse = pairsWithBothSimilarities.mapToPair(x -> new Tuple2<Object, Object>(x._2._1, x._2._2));
        RegressionMetrics metrics = new RegressionMetrics(rmse.rdd());
        System.out.println("RMSE: " + metrics.rootMeanSquaredError());

        // System.out.println();
        // System.out.println("Similar candidates: ");
        // pairsWithBothSimilarities.filter(x -> x._2._1 >= minSimilarityBroadcast.value()).foreach(s -> System.out.println(s));

    }

    private JavaPairRDD<Integer, List<Integer>> oneHot() {
        return null;
    }

    private JavaPairRDD<Integer, List<Integer>> minHash() {
        return null;
    }

    private JavaPairRDD<Integer, List<Integer>> lsh() {
        return null;
    }

}
