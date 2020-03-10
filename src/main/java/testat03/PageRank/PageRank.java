package testat03.PageRank;

import com.google.common.primitives.Doubles;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
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
        Broadcast<Double> betaBroadcast = jsc.broadcast(beta);
        JavaRDD<String> lines = jsc.textFile(path);

        // stochastische Adjazenzmatrix:

        // (i, j)
        JavaPairRDD<Long, Long> entriesTemp = lines.mapToPair(m -> {
            String[] array = m.split("\t");
            return new Tuple2<Long, Long>(Long.parseLong(array[0]), Long.parseLong(array[1]));
        }).distinct();

        // (i, Anzahl)
        JavaPairRDD<Long, Integer> columnSums = entriesTemp.mapToPair(m -> new Tuple2<Long, Integer>(m._1, 1))
                .reduceByKey((n1, n2) -> n1 + n2);

        // (j, i, d)
        JavaRDD<MatrixEntry> entries = entriesTemp.join(columnSums)
                .map(m -> new MatrixEntry(m._2._1, m._1, (1.0 / (double) m._2._2)));

        BlockMatrix adjMatrix = new CoordinateMatrix(entries.rdd()).toBlockMatrix();

        // initiale Werte:

        // initialer Vektor rOld:
        List<MatrixEntry> rOldEntries = new ArrayList<MatrixEntry>();
        for (int i = 1; i <= nodes; i++) {
            rOldEntries.add(new MatrixEntry(i, 0, 0.0));
        }
        JavaRDD<MatrixEntry> rOldEntriesRDD = jsc.parallelize(rOldEntries);
        BlockMatrix rOld = new CoordinateMatrix(rOldEntriesRDD.rdd()).toBlockMatrix();

        // initialer Vektor rNew:
        List<MatrixEntry> rNewEntries = new ArrayList<MatrixEntry>();
        for (int i = 1; i <= nodes; i++) {
            rNewEntries.add(new MatrixEntry(i, 0, 1.0));
        }
        JavaRDD<MatrixEntry> rNewEntriesRDD = jsc.parallelize(rNewEntries);
        BlockMatrix rNew = new CoordinateMatrix(rNewEntriesRDD.rdd()).toBlockMatrix();

        // PageRank berechnen (iterativ):

        // bis die Änderung kleiner als Epsilon ist:
        while (subtractVectorsAndSum(rOld, rNew) >= epsilon) {

            // rOld <- rNew:
            rOld = rNew;

            // (beta * m * r):
            BlockMatrix matrixTemp = new CoordinateMatrix(adjMatrix.toCoordinateMatrix().entries().toJavaRDD()
                    .map(m -> new MatrixEntry(m.i(), m.j(), m.value() * betaBroadcast.value())).rdd()).toBlockMatrix();
            rNew = matrixTemp.multiply(rOld);

            /*
            JavaPairRDD<Integer, Tuple2<Integer, Double>> matrixForJoin = adjMatrix.toCoordinateMatrix().entries().toJavaRDD()
                    .mapToPair(m -> new Tuple2<Integer, Tuple2<Integer, Double>>((int)m.j(), new Tuple2<Integer, Double>((int)m.i(), m.value())));
            JavaPairRDD<Integer, Double> vectorForJoin = rOld.toCoordinateMatrix().entries().toJavaRDD()
                    .mapToPair(m -> new Tuple2<Integer, Double>((int)m.i(), m.value()));
            JavaRDD<MatrixEntry> multipliedMatrices = matrixForJoin.join(vectorForJoin)
                    .mapToPair(m -> new Tuple2<Integer,Double>(m._2._1._1, betaBroadcast.value() * m._2._1._2 * m._2._2))
                    .reduceByKey((n1, n2) -> n1 + n2)
                    .map(x -> new MatrixEntry(x._1, 0, x._2));
            rNew = new CoordinateMatrix(multipliedMatrices.rdd()).toBlockMatrix();
            */

            // S = Summe aller Werte von rNew:
            double S = getMatrixSum(rNew, false);

            // für alle Werte von rNew += (1 - (S / nodes)):
            List<MatrixEntry> rAddTemp = new ArrayList<>();
            for (int i = 1; i <= nodes; i++) {
                rAddTemp.add(new MatrixEntry(i, 0, (1.0 - (S / (double) nodes))));
            }
            JavaRDD<MatrixEntry> rAddTempRDD = jsc.parallelize(rAddTemp);
            BlockMatrix rAdd = new CoordinateMatrix(rAddTempRDD.rdd()).toBlockMatrix();
            rNew = rNew.add(rAdd);

            /*
            rNew = new CoordinateMatrix(rNew.toCoordinateMatrix().entries().toJavaRDD()
                    .map(m -> new MatrixEntry(m.i(), m.j(), m.value() + (1.0 - (S / (double) nodes)))).rdd()).toBlockMatrix();
            */

        }

        // sortieren:
        JavaPairRDD<Double, Integer> ranking = rNew.toCoordinateMatrix().entries().toJavaRDD()
                .mapToPair(m -> new Tuple2<>(m.value(), (int) m.i())).sortByKey(false);

        // Ranking ausgeben (gerundet):
        ranking.take(nodeWithHighestPageRank).forEach(s -> {
            System.out.println("(" + s._2 + ", " + Math.round(100.0 * s._1) / 100.0 + ")");
        });

    }

    // Vektoren voneinander subtrahieren (mit Betrag):
    double subtractVectorsAndSum(BlockMatrix rOld, BlockMatrix rNew) {
        return getMatrixSum(rNew.subtract(rOld), true);
    }

    // alle Werte einer Blockmatrix summieren:
    double getMatrixSum(BlockMatrix matrix, boolean abs) {
        return matrix.toCoordinateMatrix().entries().toJavaRDD()
                .mapToPair(m -> new Tuple2<Integer, Double>(1, abs ? Math.abs(m.value()) : m.value()))
                .reduceByKey((n1, n2) -> n1 + n2)
                .take(1)
                .get(0)._2;
    }

}
