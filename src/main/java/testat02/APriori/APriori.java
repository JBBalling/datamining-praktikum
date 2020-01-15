package testat02.APriori;

import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Array;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * sparkSubmit --class testat02.APriori.APriori target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class APriori {

    private JavaSparkContext jsc;
    private String path = "daten/browsing.txt";
    private String output = "output/testat02/APriori";

    private final double minSupport = 0.01;
    private final double minConfidence = 0.8;

    public static void main(String[] args) throws Exception {

        APriori ap = new APriori();
        ap.aPriori();
        // ap.associationRules();
        // ap.test();
        ap.jsc.stop();

    }

    APriori() {
        SparkConf conf = new SparkConf().set("spark.executor.memory","8G");
        jsc = new JavaSparkContext(conf);
    }

    /*

    Support einer Elementmenge I (sup(I)): Anteil der Warenkörbe, welche alle Elemente aus I enthalten

    Gegeben eines Schwellenwerts s, eine Elementmenge I wird als Häufige Elementmenge bezeichnet, falls sup(I) ≥ s

    Confidence: sup(i_1, i_2, ..., i_k, j) / sup(i_1, i_2, ..., i_k)

     */

    private void aPriori() {

        Broadcast<Double> confidence = jsc.broadcast(minConfidence);
        Broadcast<Double> support = jsc.broadcast(minSupport);

        JavaRDD<String> lines = jsc.textFile(path);

        Broadcast<Long> amountOfSessions = jsc.broadcast(lines.count());

        JavaRDD<ItemSet> sessions = lines.map(s -> {
            List<String> list = Arrays.asList(s.split("\\s+"))
                    .stream()
                    .distinct()
                    .collect(Collectors.toList());
            return new ItemSet(list);
        });

        // alle vorkommenden 1-elementigen Mengen (mit Duplikaten)
        JavaRDD<ItemSet> allCandidatesWith1Element = sessions.flatMap(s -> {
            ArrayList<ItemSet> list = new ArrayList<ItemSet>();
            for (String string : s.getItems()) {
                list.add(new ItemSet(string));
            }
            return list.iterator();
        }); // .distinct();

        // alle häufigen 1-elementigen Mengen
        JavaRDD<ItemSet> frequentSetsWith1Element = allCandidatesWith1Element.mapToPair(m -> new Tuple2<ItemSet, Integer>(m, 1))
                .reduceByKey((n1, n2) -> n1 + n2)
                .filter(s -> (((double) s._2 / amountOfSessions.value()) >= support.value()))
                .keys();
        System.out.println(frequentSetsWith1Element.count());

        // alle 2-elementigen Mengen, die sich aus den 1-elementigen Mengen bilden lassen (Kandidaten)
        JavaRDD<ItemSet> candidatesWith2Elements = frequentSetsWith1Element.zipWithIndex()
                .cartesian(frequentSetsWith1Element.zipWithIndex())
                .filter(f -> f._1()._2 < f._2()._2) // Duplikate vermeiden (z.B. (3, 4) und (4, 3))
                .map(x -> {
                    ItemSet set = new ItemSet();
                    set.add(x._1._1);
                    set.add(x._2._1);
                    return set;
                });

        // alle häufigen 2-elementigen Mengen TODO zu langsam?
        JavaPairRDD<ItemSet, Integer> frequentSetsWith2ElementsCounters = candidatesWith2Elements.cartesian(sessions).flatMapToPair(c -> { // ca. 800 mio. Mal :(
            if (c._2.containsAllElements(c._1)) {
                ArrayList<Tuple2<ItemSet, Integer>> list =  new ArrayList<Tuple2<ItemSet, Integer>>();
                list.add(new Tuple2<ItemSet, Integer>(c._1, 1));
                return list.iterator();
            }
            return new ArrayList<Tuple2<ItemSet, Integer>>().iterator();
        });
        JavaRDD<ItemSet> frequentSetsWith2Elements = frequentSetsWith2ElementsCounters.reduceByKey((n1, n2) -> n1 + n2)
                .filter(x -> (x._2 / amountOfSessions.value()) >= support.value())
                .map(y -> y._1);
        frequentSetsWith2Elements.foreach(s -> System.out.println(s));
        System.out.println(frequentSetsWith2Elements.count()); // 110 ?

        // alle 3-elementigen Kandidaten TODO
        JavaRDD<ItemSet> candidatesWith3Elements = null;

        // alle häufigen 3-elementigen Mengen TODO zu langsam?
        /**
        JavaPairRDD<ItemSet, Integer> frequentSetsWith3ElementsCounters = candidatesWith3Elements.cartesian(sessions).flatMapToPair(c -> {
            if (c._2.containsAllElements(c._1)) {
                ArrayList<Tuple2<ItemSet, Integer>> list =  new ArrayList<Tuple2<ItemSet, Integer>>();
                list.add(new Tuple2<ItemSet, Integer>(c._1, 1));
                return list.iterator();
            }
            return new ArrayList<Tuple2<ItemSet, Integer>>().iterator();
        });
        JavaRDD<ItemSet> frequentSetsWith3Elements = frequentSetsWith3ElementsCounters.reduceByKey((n1, n2) -> n1 + n2)
                .filter(x -> (x._2 / amountOfSessions.value()) >= support.value())
                .map(y -> y._1);
        frequentSetsWith3Elements.foreach(s -> System.out.println(s));
        System.out.println(frequentSetsWith3Elements.count()); // 16 ?
         */

    }

    /*
     * Bestimmt alle ItemSets die häufig vorkommen
     * Nicht benutztbar (not serializable)
     */
    /**
    private void getAllFrequentItemSets(JavaRDD<ItemSet> allSessions, JavaRDD<ItemSet> input, int totalAmountOfSessions) {

        JavaPairRDD<ItemSet, Integer> allCombinations = input.cartesian(sessions).flatMapToPair(c -> {
            if (c._2.containsAllElements(c._1)) {
                ArrayList<Tuple2<ItemSet, Integer>> list =  new ArrayList<Tuple2<ItemSet, Integer>>();
                list.add(new Tuple2<ItemSet, Integer>(c._1, 1));
                return list.iterator();
            }
            return new ArrayList<Tuple2<ItemSet, Integer>>().iterator();
        });

        JavaRDD<ItemSet> frequentItems = allCombinations.reduceByKey((n1, n2) -> n1 + n2)
                .filter(x -> (x._2 / amountOfSessions.value()) >= support.value())
                .map(y -> y._1);

    }*/

    private void aPrioriOld() {

        // long amount = candidatesWith1Element.count(); // number of Product IDs (with duplicates)
        // long amountDistinct = counts.count(); // number of distinct product IDs

        double conf = minConfidence;
        double sup = minSupport;

        JavaRDD<String> lines = jsc.textFile(path);

        long amountOfSessions = lines.count();

        /*
        JavaRDD<ItemSet> input = lines.map(s -> {
            String[] sarray = s.split("\\s+");
            ItemSet itemSet = new ItemSet();
            itemSet.add(sarray);
            return itemSet;
        });
        */

        JavaPairRDD<String, Integer> candidatesWith1Element = lines.flatMapToPair(s -> {
            ArrayList<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
            List<String> l = Arrays.asList(s.split("\\s+"))
                    .stream()
                    .distinct()
                    .collect(Collectors.toList());
            for (String string : l) {
                list.add(new Tuple2<>(string, 1));
            }
            return list.iterator();
        });

        JavaRDD<String> frequentSetsWith1Element = candidatesWith1Element.reduceByKey((n1, n2) -> n1 + n2)
                .mapToPair(c -> new Tuple2<String, Double>(c._1, (double) c._2 / amountOfSessions))
                .filter(s -> s._2 >= sup)
                .keys();

        JavaPairRDD<String, String> candidatesWith2Elements = frequentSetsWith1Element.zipWithIndex()
                .cartesian(frequentSetsWith1Element.zipWithIndex())
                .filter(f -> f._1()._2 < f._2()._2)
                .mapToPair(x -> new Tuple2<String, String>(x._1._1, x._2._1));

        JavaPairRDD<Tuple2<String, String>, Integer> frequentSetsWith2Elements = candidatesWith2Elements.mapToPair(p -> new Tuple2<Tuple2<String, String>, Integer>(p, 0));

        // JavaRDD<ItemSet> allPairs = allPairsAsPairRDD.map(p -> new ItemSet(p));

        //allPairs.foreach(s -> System.out.println(s));
        //System.out.println(allPairs.count());

    }

    /**
     * Vorgegebebe Umsetzung zum Vergleichen
     */
    private void associationRules() {

        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split("\\s+"))
                .stream()
                .distinct()
                .collect(Collectors.toList()));

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSupport)
                .setNumPartitions(10);
        FPGrowthModel<String> model = fpg.run(transactions);

        for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
            System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
        }

        for (AssociationRules.Rule<String> rule : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }

    }

    private void test() {
        JavaRDD<String> test1 = jsc.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        JavaRDD<String> test2 = jsc.parallelize(Arrays.asList("x", "y", "z"));
        // test1.cartesian(test2).foreach(s -> System.out.println(s));


        JavaPairRDD<String, Integer> test3 = test1.flatMapToPair(x -> {
            if (x.equals("a") || x.equals("e") || x.equals("g")) {
                ArrayList<Tuple2<String, Integer>> al =  new ArrayList<Tuple2<String, Integer>>();
                al.add(new Tuple2<String, Integer>(x, 1));
                return al.iterator();
            }
            return new ArrayList<Tuple2<String, Integer>>().iterator();
        });

        test3.foreach(s -> System.out.println(s));
    }

}
