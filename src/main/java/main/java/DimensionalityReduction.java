package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;

import java.util.List;

/**
 * bin/spark-submit --class main.java.DimensionalityReduction /Users/jakobschwerter/Development/data-mining-praktikum/target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */
public class DimensionalityReduction {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "/Users/jakobschwerter/Documents/Uni/Moderne Datenbanktechnologien/Praktikum - Data Mining/dataminingpraktikum-master/daten/USArrests.csv";
        JavaRDD<String> lines = jsc.textFile(path);

        JavaRDD<Vector> parsedData = lines.map(s -> {
            String[] array = s.split(",");
            double[] values = new double[array.length];
            for (int i = 0; i < array.length; i++) {
                values[i] = Double.parseDouble(array[i]);
            }
            return Vectors.dense(values);
        });

        // standardisieren
        RDD<Vector> parsedDataAsRDD = parsedData.rdd();
        StandardScalerModel scalerModel = new StandardScaler(true, true).fit(parsedDataAsRDD);
        RDD<Vector> parsedDataAsRDD_scaled = scalerModel.transform(parsedDataAsRDD);
        JavaRDD<Vector> parsedData_scaled = parsedDataAsRDD_scaled.toJavaRDD();
        List<Vector> data_scaled = parsedData_scaled.collect();
        JavaRDD<Vector> rows_scaled = jsc.parallelize(data_scaled);

        RowMatrix data = new RowMatrix(rows_scaled.rdd());

        svd(data, 2);

        System.out.println();

        pca(data, 2);

        jsc.close();
    }

    static void svd(RowMatrix data, int k) {
        SingularValueDecomposition<RowMatrix, Matrix> svd = data.computeSVD(k, true, 1.0E-9d);
        RowMatrix U = svd.U();  // The U factor is a RowMatrix.
        Vector s = svd.s();     // The singular values are stored in a local dense vector.
        Matrix V = svd.V();     // The V factor is a local dense matrix.

        Vector[] collectPartitions = (Vector[]) U.rows().collect();
        System.out.println("U:");
        for (Vector vector : collectPartitions) {
            System.out.println("\t" + vector);
        }
        System.out.println("s: " + s);
        System.out.println("V:\n" + V);
    }

    static void pca(RowMatrix data, int k) {
        Matrix pc = data.computePrincipalComponents(k);
        RowMatrix projected = data.multiply(pc);
        Vector[] collectPartitions = (Vector[])projected.rows().collect();
        System.out.println("Principal components:");
        for (Vector vector : collectPartitions) {
            System.out.println("\t" + vector);
        }
    }

}
