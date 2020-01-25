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
import java.util.List;
import java.util.Random;

/**
 * sparkSubmit --class testat02.MinHashingAndLSH.LSH target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class LSH implements java.io.Serializable {

    private SparkConf conf;
    private JavaSparkContext jsc;

    static int numberHashFunctions = 1000;
    private int k = 3;
    private int bands = 20;
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

    // sparkSubmit --conf "spark.driver.extraJavaOptions=-Xms4g" --executor-memory 4g --driver-memory 4g --class testat02.MinHashingAndLSH.LSH target/data-mining-praktikum-1.0-SNAPSHOT.jar
    void main() {

        conf = new SparkConf().set("spark.executor.memory", "8G");
        jsc = new JavaSparkContext(conf);
        Broadcast<Integer> shingleSize = jsc.broadcast(k);
        Broadcast<Integer> bandsBroadcast = jsc.broadcast(bands);
        Broadcast<Integer> rowsBroadcast = jsc.broadcast(rows);
        Broadcast<Double> minSimilarityBroadcast = jsc.broadcast(minSimilarity);

        // Daten einlesen
        JavaRDD<Review> lines = jsc.parallelize(jsc.textFile(path).take(1000)) // jsc.textFile(path) für alle Zeilen
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

        // one hot Kodierung
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

        JavaPairRDD<Integer, List<Integer>> signatures = oneHot.mapToPair(m -> {
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
        JavaRDD<Tuple2<Integer, Integer>> similiarDocuments = signaturesDividedInBands.groupByKey()
                .map(m -> Lists.newArrayList(m._2))
                .filter(f -> f.size() > 1)
                .flatMap(p -> {
                    List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                    for (int i = 0; i < p.size(); i++) {
                        for (int j = i + 1; j < p.size(); j++) {
                            Tuple2<Integer, Integer> tuple = new Tuple2<>(p.get(i), p.get(j));
                            list.add(tuple);
                        }
                    }
                    return list.iterator();
                }).distinct(); // distinct?

        JavaPairRDD<Integer, Tuple2<List<Integer>, List<Integer>>> oneHotAndSignatures = oneHot.join(signatures); // (id, (oneHot, signature))

        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Integer>, Tuple2<List<Integer>, List<Integer>>>> pairsOneHotAndSignaturesTemp = similiarDocuments.flatMapToPair(f -> {
            List<Tuple2<Integer, Tuple2<Integer, Tuple2<Integer, Integer>>>> list = new ArrayList<>();
            int randomNumber = new Random().nextInt(Integer.MAX_VALUE);
            list.add(new Tuple2<Integer, Tuple2<Integer, Tuple2<Integer, Integer>>>(f._1, new Tuple2<Integer, Tuple2<Integer, Integer>>(randomNumber, f)));
            list.add(new Tuple2<Integer, Tuple2<Integer, Tuple2<Integer, Integer>>>(f._2, new Tuple2<Integer, Tuple2<Integer, Integer>>(randomNumber, f)));
            return list.iterator();
        })
                .join(oneHotAndSignatures)
                .mapToPair(p -> {
                    int randID = p._2._1._1;
                    Tuple2<Integer, Integer> pair = p._2._1._2;
                    Tuple2<List<Integer>, List<Integer>> lists = p._2._2;
                    return new Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Tuple2<List<Integer>, List<Integer>>>>(randID, new Tuple2<Tuple2<Integer, Integer>, Tuple2<List<Integer>, List<Integer>>>(pair, lists));
                });

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Tuple2<List<Integer>, List<Integer>>, Tuple2<List<Integer>, List<Integer>>>> pairsOneHotAndSignatures = pairsOneHotAndSignaturesTemp.join(pairsOneHotAndSignaturesTemp)
                .mapToPair(x -> {
                    Tuple2<Integer, Integer> pair = x._2._1._1;
                    Tuple2<Tuple2<List<Integer>, List<Integer>>, Tuple2<List<Integer>, List<Integer>>> listsOfBoth = new Tuple2<Tuple2<List<Integer>, List<Integer>>, Tuple2<List<Integer>, List<Integer>>>(x._2._1._2, x._2._2._2);
                    return new Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<List<Integer>, List<Integer>>, Tuple2<List<Integer>, List<Integer>>>>(pair, listsOfBoth);
                });



        /*
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
*/



        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> pairsWithBothSimilarities = pairsOneHotAndSignatures.mapToPair(p -> {
            double jaccard = jaccardSimilarity(p._2._1._1, p._2._2._1);
            double minHash = jaccardSimilarity2(p._2._1._2, p._2._2._2);
            return new Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>(p._1, new Tuple2<Double, Double>(jaccard, minHash));
            /*
            double jaccard = jaccardSimilarity(oneHotListBroadcast.value().get(p.get(0) - 1), oneHotListBroadcast.value().get(p.get(1) - 1));
            double minHash = jaccardSimilarity2(signaturesListBroadcast.value().get(p.get(0) - 1), signaturesListBroadcast.value().get(p.get(1) - 1));
            return new Tuple2<ArrayList<Integer>, Tuple2<Double, Double>>(p, new Tuple2<Double, Double>(jaccard, minHash));
            */
        });

                // ???


                // .distinct(); // .filter(x -> x._2._1 > minSimilarityBroadcast.value());

        pairsWithBothSimilarities.foreach(s -> System.out.println(s));

        JavaPairRDD<Object, Object> rmse = pairsWithBothSimilarities.mapToPair(x -> new Tuple2<Object, Object>(x._2._1, x._2._2));
        RegressionMetrics metrics = new RegressionMetrics(rmse.rdd());
        System.out.println(metrics.rootMeanSquaredError());

        System.out.println(pairsWithBothSimilarities.count());
        pairsWithBothSimilarities.filter(x -> x._2._1 > minSimilarityBroadcast.value()).foreach(s -> System.out.println(s));

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
