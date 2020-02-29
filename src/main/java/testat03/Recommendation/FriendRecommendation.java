package testat03.Recommendation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * sparkSubmit --class testat03.Recommendation.FriendRecommendation target/data-mining-praktikum-1.0-SNAPSHOT.jar
 */

public class FriendRecommendation implements java.io.Serializable {
	
	private transient SparkConf conf;
    private transient JavaSparkContext jsc;

    private static int friendNumber = 10;
    private String path = "daten/soc-LiveJournal1Adj.txt";
    
	public static void main(String[] args) {
		FriendRecommendation recSystem = new FriendRecommendation();
		recSystem.conf = new SparkConf().set("spark.executor.memory", "8G");
		recSystem.jsc = new JavaSparkContext(recSystem.conf);
		recSystem.recommend(friendNumber);
	}
	
	public void recommend(int amount) {
		JavaRDD<String> lines = jsc.textFile(path);
		JavaRDD<User> users = lines.map(f -> new User(f));

		JavaPairRDD<Tuple2<Integer, Integer>, Integer> friendEdges = users.flatMapToPair(f -> {
			List<Tuple2<Tuple2<Integer, Integer>, Integer>> connectedFriends = friendShipConnection(f);
			return connectedFriends.iterator();
		});
		friendEdges.cache();

		JavaPairRDD<Tuple2<Integer, Integer>, Integer> recs = friendEdges
				.filter(f -> {
					return true;
				})
				.reduceByKey((n1, n2) -> n1 + n2);

		recs.foreach(s -> System.out.println(s));
		
	}

	/*
	 * Maps an friendship to two individuals (id_1, id_2, flag(friends?) -> friends = {0;1}, 0 = already friends together, 1 = have a friend together
	 * Output will be a list of Tuples ((userId, userId), friends?)
	 */
	public List<Tuple2<Tuple2<Integer, Integer>, Integer>> friendShipConnection(User user) {
		List<Tuple2<Tuple2<Integer, Integer>, Integer>> connections = new ArrayList<>();
		Tuple2<Integer, Integer> key;

		// direkte Freundschaften
		for (int friend : user.getFriends()) {
			key = new Tuple2<Integer, Integer>(user.getUserID(), friend);
			if (user.getUserID() > friend) {
				key = new Tuple2<Integer, Integer>(friend, user.getUserID());
			}
			connections.add(new Tuple2<Tuple2<Integer, Integer>, Integer>(key, 0));
		}
		
		// Freunde mit gemeinsamen Freunden
		List<Tuple2<Integer, Integer>> combinations = getPossibleCombinations(user.getFriends());
		for (Tuple2<Integer, Integer> friendPair : combinations) {
			connections.add(new Tuple2<>(friendPair, 1));
		}
		
		return connections;
	}

	// alle möglichen Kombinationen
	List<Tuple2<Integer, Integer>> getPossibleCombinations(Set<Integer> set) {
		List<Integer> list = new ArrayList<>(set);
        List<Tuple2<Integer, Integer>> combinations = new ArrayList<>();
        for (int i = 0; i < list.size() - 1; i++) {
        	for (int j = i + 1; j < list.size(); j++) {
        		if (list.get(i) <= list.get(j)) {
					combinations.add(new Tuple2<Integer, Integer>(list.get(i), list.get(j)));
				} else {
					combinations.add(new Tuple2<Integer, Integer>(list.get(j), list.get(i)));
				}
        	}
        }
        return combinations;
    }

}
