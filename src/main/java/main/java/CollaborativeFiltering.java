package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.*;

public class CollaborativeFiltering {

    public static void main(String[] args) {

        collaborativeFiltering(0, 1);

    }

    static void collaborativeFiltering(int kMostSimiliarObjects, int userID) {

        SparkConf conf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path = "/Users/jakobschwerter/Documents/Uni/Moderne Datenbanktechnologien/Praktikum - Data Mining/dataminingpraktikum-master/daten/ratings.txt";




        // Load and parse the data

        JavaRDD<String> lines = jsc.textFile(path);
        JavaRDD<Vector> rows = lines.map(s -> {
            String[] sarray = s.split("\\s+");
            return Vectors.dense(
                    Integer.parseInt(sarray[0]),
                    Integer.parseInt(sarray[1]),
                    Double.parseDouble(sarray[2]));
        });

        JavaRDD<Vector> training = rows.sample(false, 0.8);
        training.cache();
        JavaRDD<Vector> test = rows.subtract(training);
        test.cache();



        RowMatrix rowMat = new RowMatrix(training.rdd());



        long m = rowMat.numRows();
        long n = rowMat.numCols();


        System.out.println("rows: " + m + ", cols: " + n);



        RowMatrix rowMatTransposed = transposeRM(jsc, rowMat);

        CoordinateMatrix coords = rowMatTransposed.columnSimilarities();





        /*
        for(Vector v : rows.collect()){
            System.out.println(v);
        }
        */

        jsc.stop();

    }


    /**
     * Quelle: https://stackoverflow.com/a/47548453/9606588
     */
    public static RowMatrix transposeRM(JavaSparkContext jsc, RowMatrix mat) {
        List<Vector> newList = new ArrayList<Vector>();
        List<Vector> vs = mat.rows().toJavaRDD().collect();
        double [][] tmp = new double[(int)mat.numCols()][(int)mat.numRows()] ;

        for(int i=0; i < vs.size(); i++){
            double[] rr=vs.get(i).toArray();
            for(int j=0; j < mat.numCols(); j++){
                tmp[j][i]=rr[j];
                tmp[i][j]=rr[j];
            }
        }

        for(int i=0; i < mat.numCols();i++)
            newList.add(Vectors.dense(tmp[i]));

        JavaRDD<Vector> rows2 = jsc.parallelize(newList);
        RowMatrix newmat = new RowMatrix(rows2.rdd());
        return (newmat);
    }



}

