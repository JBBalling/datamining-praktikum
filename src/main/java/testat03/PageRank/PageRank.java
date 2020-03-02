package testat03.PageRank;

import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.GridPartitioner;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import scala.Tuple2;
import testat03.Recommendation.User;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * sparkSubmit --class testat03.PageRank.PageRank target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class PageRank implements java.io.Serializable {

    private transient SparkConf conf;
    private transient JavaSparkContext jsc;

    private String path = "daten/graph.txt";
    private String output = "output/testat03/PageRank";

    private double beta = 0.8;
    private double epsilon = 1.0;

    private int nodes = 1000;
    private int l = 100;

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
            return new MatrixEntry(Long.parseLong(array[0])-1, Long.parseLong(array[1])-1, 1); // -1, da es bei 0 beginnen soll
        });
        CoordinateMatrix coordMat = new CoordinateMatrix(entries.rdd());
        BlockMatrix blockMatrix = coordMat.toBlockMatrix(l, l).cache();

        /*
        double[] rNewArray = new double[nodes];
        for (int i = 0; i < rNewArray.length; i++) {
            rNewArray[i] = 1;
        }
        Vector rNew = Vectors.dense(rNewArray);
        Vector rOld = Vectors.zeros(nodes);
        */

        /*
        List<MatrixEntry> listOld = new ArrayList<>();
        List<MatrixEntry> listNew = new ArrayList<>();
        for (int i = 0; i < nodes; i++) {
            listOld.add(new MatrixEntry(i, 1, 1));
            listNew.add(new MatrixEntry(i, 1, 3));
        }
        JavaRDD<MatrixEntry> rOldEntries = jsc.parallelize(listOld);
        JavaRDD<MatrixEntry> rNewEntries = jsc.parallelize(listNew);
        CoordinateMatrix coordMatrOld = new CoordinateMatrix(rOldEntries.rdd());
        CoordinateMatrix coordMatrNew = new CoordinateMatrix(rNewEntries.rdd());
        BlockMatrix vectorROld = coordMatrOld.toBlockMatrix(l, l).cache();
        BlockMatrix vectorRNew = coordMatrNew.toBlockMatrix(l, l).cache();

        System.out.println(Arrays.toString(vectorRNew.subtract(vectorROld).toLocalMatrix().toArray()));
        */

        List<Tuple2<Integer, Double>> listOld = new ArrayList<>();
        List<Tuple2<Integer, Double>> listNew = new ArrayList<>();
        for (int i = 0; i < nodes; i++) {
            listOld.add(new Tuple2<Integer, Double>(i, 0.0));
            listNew.add(new Tuple2<Integer, Double>(i, 1.0));
        }
        JavaPairRDD<Integer, Double> rOld = jsc.parallelizePairs(listOld).sortByKey(true);
        JavaPairRDD<Integer, Double> rNew = jsc.parallelizePairs(listNew).sortByKey(true);
        rOld.partitionBy(new GridPartitioner(nodes/l, 1, l, 1));
        rNew.partitionBy(new GridPartitioner(nodes/l, 1, l, 1));

        System.out.println(rNew.partitions());



    }

}
