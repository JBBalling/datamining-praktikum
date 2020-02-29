package testat03.PageRank;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import testat03.Recommendation.User;

/**
 * sparkSubmit --class testat03.PageRank.PageRank target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class PageRank {

    private transient SparkConf conf;
    private transient JavaSparkContext jsc;

    private String path = "daten/graph.txt";
    private String output = "output/testat03/PageRank";

    private double beta = 0.8;
    private double epsilon = 1.0;

    private int nodes = 1000;

    private int nodeWithHighestPageRank = 10;

    public static void main(String[] args) {
        PageRank pageRank = new PageRank();
        pageRank.conf = new SparkConf().set("spark.executor.memory", "8G");
        pageRank.jsc = new JavaSparkContext(pageRank.conf);
        pageRank.main();
    }

    void main() {
        JavaRDD<String> lines = jsc.textFile(path);
        JavaRDD<MatrixEntry> entries = lines.map(m -> {
            String[] array = m.split("\t");
            return new MatrixEntry(Long.parseLong(array[0]), Long.parseLong(array[1]), 1);
        });
        CoordinateMatrix coordMat = new CoordinateMatrix(entries.rdd());
        BlockMatrix blockMatrix = coordMat.toBlockMatrix().cache();

        double[] rNewArray = new double[nodes];
        for (int i = 0; i < rNewArray.length; i++) {
            rNewArray[i] = 1;
        }
        Vector rNew = Vectors.dense(rNewArray);
        Vector rOld = Vectors.zeros(nodes);

    }


}
