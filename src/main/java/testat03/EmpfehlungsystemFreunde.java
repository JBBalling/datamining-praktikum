package main.java;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.linalg.Vectors;

import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import java.util.stream.Collectors; 
import java.util.stream.StreamSupport; 

import scala.Tuple2;

import org.apache.spark.mllib.linalg.distributed.IndexedRow;

/**
 * bin\spark-submit --class main.java.EmpfehlungsystemFreunde C:\Users\balli\eclipse-workspace\DataMining\target\DataMining-0.0.1-SNAPSHOT.jar 
 * @param <Review>
 */

public class EmpfehlungsystemFreunde implements java.io.Serializable {
	
	private transient SparkConf conf;
    private transient JavaSparkContext jsc;
    private String path = "C:/Users/balli/spark/soc-LiveJournal1Adj.txt";
    private int anzahlFreunde = 10;
    
	
	public static void main(String[] args) {
		EmpfehlungsystemFreunde empfehlungssystem = new EmpfehlungsystemFreunde();
		empfehlungssystem.conf = new SparkConf().set("spark.executor.memory", "8G");
		empfehlungssystem.jsc = new JavaSparkContext(empfehlungssystem.conf);
		empfehlungssystem.empfehle(empfehlungssystem.anzahlFreunde);
	}
	
	public void empfehle(int anzahl) {
		JavaRDD<String> lines = jsc.textFile(path);
		JavaRDD<Tuple2<Integer, List<Integer>>> rowFriends = lines.map(f -> {
			String[] array = f.split("\t");
			int index = Integer.parseInt(array[0]);
			//System.out.println(array.toString());
			String[] friends = array[1].split(",");
			List<Integer> friendsList = new ArrayList<>();
			for (int i = 0; i < friends.length; i++) {
				friendsList.add(Integer.parseInt(friends[i]));
			}
			return new Tuple2<Integer, List<Integer>>(index, friendsList);
		});
		
		//row.foreach(f -> System.out.println(f.toString()));
		//IndexedRowMatrix idxMat = new IndexedRowMatrix(row.rdd());
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> friend_edges = rowFriends.flatMapToPair(f -> {
			List<Tuple2<Tuple2<Integer, Integer>, Integer>> connectedFriends = friendShipConnection(f);
			return connectedFriends.iterator();
		});
		friend_edges.cache();
		
		friend_edges.groupByKey().foreach(f -> System.out.println(f));
		/*
		JavaRDD<Object> friendsOfFriends = friend_edges.groupByKey().filter(f -> {
			List<Integer> list = StreamSupport 
                    .stream(f._2().spliterator(), false) 
                    .collect(Collectors.toList());
			return list.retainAll([0])
		}).map(x -> {
			int sum = 0;
			if (x._2().iterator().hasNext()) {
				sum += x._2().iterator().next();
			}
			return new Tuple2<>(x._1, sum);
		});
		friendsOfFriends.foreach(f -> System.out.println(f));*/
		//friend_edges.foreach(f -> System.out.println(f));
		
		//friendsOfFriends = friend_edges.
		
		
	}
				
				
				
				
				
				
	/*
	 * Maps an friendship to two individuals (id_1, id_2, flag(friends?) -> friends = {0;1}, 0 = already friends together, 1 = have a friend together
	 * Output will be a list of Tuples ((userId, userId), friends?)
	 */
	public List<Tuple2<Tuple2<Integer, Integer>, Integer>> friendShipConnection(Tuple2<Integer, List<Integer>> friendships) {
		int userID = friendships._1();
		List<Integer> friendList = friendships._2();
		Tuple2<Integer, Integer> key;
		List<Tuple2<Tuple2<Integer, Integer>, Integer>> connection = new ArrayList<>();
		for (int friend : friendList) {
			
			key = new Tuple2<>(userID, friend);
			if (userID > friend) {
				key = new Tuple2<>(friend, userID);
			}
			connection.add(new Tuple2<>((key), 0));
		}
		
		// Freunde mit gemeinsamen Freunden
		List<Tuple2<Integer, Integer>> combinatedFriends = getPossibleCombinations(friendList);
		for (Tuple2<Integer, Integer> friendPair : combinatedFriends) {
			int friend1 = friendPair._1();
			int friend2 = friendPair._2();
			key = new Tuple2<>(friend1, friend2);
			if (friend1 > friend2) {
				key = new Tuple2<>(friend2, friend1);
			}
			connection.add(new Tuple2<>(key, 1));
		}
		
		return connection;
	}
	List<Tuple2<Integer, Integer>> getPossibleCombinations(List<Integer> list) {
        List<Tuple2<Integer, Integer>> combinations = new ArrayList<>();
        for (int i = 0; i < list.size() - 1; i++) {
        	for (int j = i + 1; j < list.size(); j++) {
        		combinations.add(new Tuple2<>(list.get(i), list.get(j)));
        	}
        }
        return combinations;
    }
}
