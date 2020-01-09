package testat02.APriori;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
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

}
