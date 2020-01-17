package testat02.MinHashingAndLSH;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * sparkSubmit --class testat02.MinHashingAndLSH.LSH target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class LSH implements java.io.Serializable {

    private String path = "/Users/jakobschwerter/Development/data-mining-praktikum/daten/imdb.txt";

    SparkConf conf;
    JavaSparkContext jsc;

    private int k = 3;
    private int minHashSignatureSize = 1000;
    private int primeNumber = 131071;

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

        lines.foreach(x -> System.out.println(x));
    }


}
