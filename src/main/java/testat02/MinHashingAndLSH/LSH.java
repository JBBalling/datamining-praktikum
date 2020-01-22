package testat02.MinHashingAndLSH;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import com.google.common.hash.*;

import java.util.ArrayList;
import java.util.List;

/**
 * sparkSubmit --class testat02.MinHashingAndLSH.LSH target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class LSH implements java.io.Serializable {

    private String path = "/Users/jakobschwerter/Development/data-mining-praktikum/daten/imdb.txt";

    private SparkConf conf;
    private JavaSparkContext jsc;

    private int k = 3;

    private int numberHashFunctions = 1000;

    private double minSimilarity = 0.8;

    private int bands = 500;
    private int rows = numberHashFunctions / bands;

    private static int minHashSignatureSize = 1000;
    private static int primeNumber = 131071;

    public static void main(String[] args) {
        LSH lsh = new LSH();
        lsh.main();
    }

    static double jaccardDistance(List<Integer> list1, List<Integer> list2) throws Exception {
        if (list1.size() != list2.size()) {
            throw new Exception();
        }
        int notSame = 0;
        for (int i = 0; i < list1.size(); i++) {
            if (!list1.get(i).equals(list2.get(i))) {
                notSame++;
            }
        }
        return (double) notSame / (double) list1.size();
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

        Broadcast<Integer> bandsBroadcast = jsc.broadcast(bands);
        Broadcast<Integer> rowsBroadcast = jsc.broadcast(rows);

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

        JavaRDD<ArrayList<Integer>> similiarDocuments = signaturesDividedInBands.groupByKey()
                .map(m -> Lists.newArrayList(m._2))
                .filter(f -> f.size() > 1)
                .flatMap(p -> {
                    List<ArrayList<Integer>> list = new ArrayList<>();
                    for (int i = 0; i < p.size(); i++) {
                        for (int j = i + 1; j < p.size(); j++) {
                            ArrayList<Integer> list2 = new ArrayList<>();
                            list2.add(p.get(i));
                            list2.add(p.get(j));
                            list.add(list2);
                        }
                    }
                    return list.iterator();
                });

        List<List<Integer>> oneHotList = new ArrayList<>((int) oneHot.count());
        for (Tuple2<Integer, List<Integer>> tuple : oneHot.sortByKey(true).collect()) {
            oneHotList.add(tuple._1 - 1, tuple._2);
        }
        Broadcast<List<List<Integer>>> oneHotListBroadcast = jsc.broadcast(oneHotList);

        List<List<Integer>> signaturesList = new ArrayList<>((int) signatures.count());
        for (Tuple2<Integer, List<Integer>> tuple : signatures.sortByKey(true).collect()) {
            signaturesList.add(tuple._1 - 1, tuple._2);
        }
        Broadcast<List<List<Integer>>> signaturesListBroadcast = jsc.broadcast(signaturesList);

        Broadcast<Double> minSimilarityBroadcast = jsc.broadcast(minSimilarity);

        JavaPairRDD<ArrayList<Integer>, Tuple2<Double, Double>> pairsOneHot = similiarDocuments.mapToPair(p -> {
            double jaccard = jaccardDistance(oneHotListBroadcast.value().get(p.get(0) - 1), oneHotListBroadcast.value().get(p.get(1) - 1));
            double minHash = jaccardDistance(signaturesListBroadcast.value().get(p.get(0) - 1), signaturesListBroadcast.value().get(p.get(1) - 1));
            return new Tuple2<ArrayList<Integer>, Tuple2<Double, Double>>(p, new Tuple2<Double, Double>(jaccard, minHash));
        }); //.filter(x -> x._2._1 > minSimilarityBroadcast.value());

        pairsOneHot.foreach(s -> System.out.println(s));

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
