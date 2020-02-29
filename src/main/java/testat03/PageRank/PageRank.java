package testat03.PageRank;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * sparkSubmit --class testat03.PageRank.PageRank target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class PageRank {

    private transient SparkConf conf;
    private transient JavaSparkContext jsc;

    private String path = "daten/graph.txt";
    private String output = "output/testat03/PageRank";

    public static void main(String[] args) {
        PageRank pageRank = new PageRank();
        pageRank.conf = new SparkConf().set("spark.executor.memory", "8G");
        pageRank.jsc = new JavaSparkContext(pageRank.conf);
    }


}
