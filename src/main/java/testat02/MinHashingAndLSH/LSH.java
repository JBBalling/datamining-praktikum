package testat02.MinHashingAndLSH;

import com.google.common.collect.Lists;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import scala.Tuple2;
import com.google.common.hash.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * sparkSubmit --conf "spark.driver.extraJavaOptions=-Xms4g" --class testat02.MinHashingAndLSH.LSH target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class LSH implements java.io.Serializable {

    private SparkConf conf;
    private JavaSparkContext jsc;

    static final int numberHashFunctions = 1000;
    private final int k = 3;
    private final double minSimilarity = 0.8;

    private final String path = "/Users/jakobschwerter/Development/data-mining-praktikum/daten/imdb.txt";

    public static void main(String[] args) {

        LSH lsh = new LSH();
        lsh.conf = new SparkConf().set("spark.executor.memory", "8G");
        lsh.jsc = new JavaSparkContext(lsh.conf);

        Set<Tuple2<Integer, Integer>> referenceSet = getReferenceSet();
        getSetPercentage(lsh.findPairs(20), referenceSet); // ~30 Paare & ~21/22 gefunden
        // getSetPercentage(lsh.findPairs(25), referenceSet); // ~140 Paare & ~21/22 gefunden

        // 22/22 gefundene Paare: ab ~35 Bänder

        // alle Paare finden
        /*
        HashSet<Tuple2<Integer, Integer>> pairsSet = new HashSet<>();
        for (int i = 1; i < 55; i++) {
            pairsSet.addAll(lsh.findPairs(i));
            System.out.println("\nAll Pairs up to " + i + " bands: " + pairsSet);
        }
        System.out.println("\nAll pairs: ");
        System.out.println(pairsSet);
        */

        // Bänderanzahl optimieren
        /*
        Set<Tuple2<Integer, Integer>> referenceSet = getReferenceSet();
        for (int i = 5; i <= 50; i += 5) {
            getSetPercentage(lsh.findPairs(i), referenceSet);
        }
        */

        lsh.jsc.stop();

    }

    /**
     * Berechnet die Jaccard-Ähnlichkeit von zwei OneHot-Kodierungen
     */
    static double jaccardSimilarityForOneHot(Set<Integer> set1, Set<Integer> set2) {
        Set<Integer> both = new HashSet<Integer>(set1);
        both.addAll(set2);
        Set<Integer> intersection = new HashSet<Integer>(set1);
        intersection.retainAll(set2);
        return (double) intersection.size() / (double) both.size();
    }

    /**
     * Berechnet die Jaccard-Ähnlichkeit von zwei MinHash-Signaturen
     */
    static double jaccardSimilarityForMinHash(List<Integer> list1, List<Integer> list2) {
        if (list1.size() != list2.size()) {
            System.err.println("Error: Wrong Size");
        }
        int same = 0;
        for (int i = 0; i < list1.size(); i++) {
            if (list1.get(i).equals(list2.get(i))) {
                same++;
            }
        }
        return (double) same / (double) list1.size();
    }

    /**
     * Gibt ein Set mit allen Paaren zurück
     */
    static Set<Tuple2<Integer, Integer>> getReferenceSet() {

        int[][] referencePairsArray = new int[][]{
                new int[]{6194,6195},
                new int[]{6194,6197},
                new int[]{9755,9772},
                new int[]{6176,6177},
                new int[]{7208,7209},
                new int[]{793,848},
                new int[]{6227,6229},
                new int[]{454,458},
                new int[]{4453,4454},
                new int[]{2859,2870},
                new int[]{8199,8209},
                new int[]{6177,6183},
                new int[]{1739,1754},
                new int[]{629,631},
                new int[]{9385,9386},
                new int[]{6176,6183},
                new int[]{9552,9553},
                new int[]{8392,8403},
                new int[]{6195,6197},
                new int[]{7091,7093},
                new int[]{2733,2734},
                new int[]{9379,9382}
        };
        Set<Tuple2<Integer, Integer>> referencePairs = new HashSet<Tuple2<Integer, Integer>>();
        for (int[] ref : referencePairsArray) {
            referencePairs.add(new Tuple2<Integer, Integer>(ref[0], ref[1]));
        }
        System.out.println("All pairs (for reference): " + referencePairs + " (amount: " + referencePairs.size() + ")");
        return referencePairs;
    }

    /**
     * Berechnet den Anteil der Elemente des referenceSets, der in der Liste list enthalten ist
     */
    static double getSetPercentage(List<Tuple2<Integer, Integer>> list, Set<Tuple2<Integer, Integer>> referenceSet) {
        int amountFound = 0;
        for (Tuple2<Integer, Integer> tuple : referenceSet) {
            if (list.contains(tuple)) {
                amountFound++;
            }
        }
        double result = (double) amountFound / (double) referenceSet.size();
        System.out.println("Pairs found: " + (result * 100) + "%");
        return result;
    }

    /**
     * Findet Paare in einem Datensatz und gibt sie als Liste zurück.
     * Die Anzahl der Bänder werden als Parameter übergeben.
     */
    List<Tuple2<Integer, Integer>> findPairs(int bandsParam) {

        int bands = bandsParam;
        int rows = numberHashFunctions / bands;

        Broadcast<Integer> shingleSize = jsc.broadcast(k);
        Broadcast<Integer> bandsBroadcast = jsc.broadcast(bands);
        Broadcast<Integer> rowsBroadcast = jsc.broadcast(rows);
        Broadcast<Double> minSimilarityBroadcast = jsc.broadcast(minSimilarity);

        System.out.println("\nBands: " + bandsParam);

        // Daten einlesen
        JavaRDD<Review> lines = jsc.parallelize(jsc.textFile(path).take(10000))
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
        JavaPairRDD<Integer, Set<Integer>> oneHot = lines.mapToPair(l -> { // (documentID, OneHot)
            Set<Integer> oneHotSet = new HashSet<Integer>();
            for (int i = 0; i < oneHotReference.value().length; i++) {
                if (l.getShingles().contains(oneHotReference.value()[i])) {
                    oneHotSet.add(i);
                }
            }
            return new Tuple2<Integer, Set<Integer>>(l.getID(), oneHotSet);
        }).partitionBy(new HashPartitioner(10));
        oneHot.cache();

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
            for (Integer i : m._2) {
                for (int j = 0; j < permutationsBroadcast.value().get(i).size(); j++) {
                    if (permutationsBroadcast.value().get(i).get(j) < sig.get(j)) {
                        sig.set(j, permutationsBroadcast.value().get(i).get(j));
                    }
                }
            }
            return new Tuple2<Integer, List<Integer>>(m._1, sig);
        }).partitionBy(new HashPartitioner(10));
        signatures.cache();

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
                            int left = p.get(i) < p.get(j) ? p.get(i) : p.get(j);
                            int right = p.get(j) > p.get(i) ? p.get(j) : p.get(i);
                            Tuple2<Integer, Integer> tuple = new Tuple2<>(left, right);
                            list.add(tuple);
                        }
                    }
                    return list.iterator();
                }).distinct();

        System.out.println("Probable pairs: " + similiarDocuments.count());

        // Jaccard- und MinHash-Ähnlichkeiten berechnen
        List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> pairsWithSimilarities = new ArrayList<>();
        for (Tuple2<Integer, Integer> p : similiarDocuments.collect()) {
            double jaccard = jaccardSimilarityForOneHot(oneHot.lookup(p._1).get(0), oneHot.lookup(p._2).get(0));
            double minHash = jaccardSimilarityForMinHash(signatures.lookup(p._1).get(0), signatures.lookup(p._2).get(0));
            pairsWithSimilarities.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>(p, new Tuple2<Double, Double>(jaccard, minHash)));
        }
        JavaRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> pairsWithBothSimilarities = jsc.parallelize(pairsWithSimilarities);

        /*
        // RMSE
        JavaPairRDD<Object, Object> rmse = pairsWithBothSimilarities.mapToPair(x -> new Tuple2<Object, Object>(x._2._1, x._2._2));
        RegressionMetrics metrics = new RegressionMetrics(rmse.rdd());
        System.out.println("RMSE: " + metrics.rootMeanSquaredError());
        */

        // Paare mit Jaccard-Ähnlichkeit >= 0,8
        JavaPairRDD<Integer, Integer> referencePairs = pairsWithBothSimilarities.filter(f -> f._2._1 >= minSimilarityBroadcast.value())
                .mapToPair(m -> m._1);
        List<Tuple2<Integer, Integer>> result = referencePairs.collect();
        System.out.println("Found pairs: " + result + " (amount: " + result.size() + ")");

        return result;

    }

}
