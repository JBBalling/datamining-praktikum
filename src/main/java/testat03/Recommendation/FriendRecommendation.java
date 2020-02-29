package testat03.Recommendation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
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
    private String output = "output/testat03/FriendRecommendation";
    
	public static void main(String[] args) {
		FriendRecommendation recSystem = new FriendRecommendation();
		recSystem.conf = new SparkConf().set("spark.executor.memory", "8G");
		recSystem.jsc = new JavaSparkContext(recSystem.conf);
		recSystem.recommend(friendNumber);
	}
	
	public void recommend(int amount) {
		JavaRDD<String> lines = jsc.textFile(path);
		JavaRDD<User> users = lines.map(f -> new User(f));

		JavaPairRDD<Integer, List<Tuple2<Integer, Integer>>> usersWithoutFriends = users.filter(f -> f.getFriends().size() == 0)
                .mapToPair(m -> new Tuple2<Integer, List<Tuple2<Integer, Integer>>>(m.getUserID(), new ArrayList<Tuple2<Integer, Integer>>()));

		JavaPairRDD<Tuple2<Integer, Integer>, Integer> friendEdges = users.flatMapToPair(f -> {
			List<Tuple2<Tuple2<Integer, Integer>, Integer>> connectedFriends = friendShipConnection(f);
			return connectedFriends.iterator();
		});
		friendEdges.cache();

		JavaPairRDD<Tuple2<Integer, Integer>, Integer> sharedFriends = friendEdges.groupByKey()
				.filter(f -> {
				    Iterator<Integer> iterator = f._2.iterator();
				    while (iterator.hasNext()) {
				        if (iterator.next() == 0) {
				            return false;
                        }
                    }
					return true;
				})
                .mapToPair(m -> {
                    int sum = 0;
                    Iterator<Integer> iterator = m._2.iterator();
                    while (iterator.hasNext()) {
                        sum += iterator.next();
                    }
                    return new Tuple2<Tuple2<Integer, Integer>, Integer>(m._1, sum);
                });

        JavaPairRDD<Integer, List<Tuple2<Integer, Integer>>> usersWithRecommendedUsers = sharedFriends.flatMapToPair(f -> {
            List<Tuple2<Integer, Tuple2<Integer, Integer>>> list = new ArrayList<>();
            list.add(new Tuple2<Integer, Tuple2<Integer, Integer>>(f._1._1, new Tuple2<Integer, Integer>(f._1._2, f._2)));
            list.add(new Tuple2<Integer, Tuple2<Integer, Integer>>(f._1._2, new Tuple2<Integer, Integer>(f._1._1, f._2)));
            return list.iterator();
        })
                .groupByKey()
                .mapToPair(p -> {
                    List<Tuple2<Integer, Integer>> list = Lists.newArrayList(p._2);
                    Collections.sort(list, new CustomComparator());
                    list = list.subList(0, Math.min(list.size(), amount));
                    return new Tuple2<Integer, List<Tuple2<Integer, Integer>>>(p._1, list);
                });

        JavaPairRDD<Integer, String> allUsers = usersWithRecommendedUsers.union(usersWithoutFriends)
                .mapToPair(m -> {
                    StringBuilder str = new StringBuilder();
                    str.append(m._1);
                    str.append("\t");
                    boolean begin = true;
                    for (Tuple2<Integer, Integer> tuple : m._2) {
                        if (!begin) {
                            str.append(",");
                        }
                        begin = false;
                        str.append(tuple._1);
                    }
                    return new Tuple2<Integer, String>(m._1, str.toString());
                })
                .sortByKey(true);

        allUsers.map(m -> m._2).saveAsTextFile(output);

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

// Comparator um Tuple zu sortieren
class CustomComparator implements Comparator<Tuple2<Integer, Integer>> {
    @Override
    public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
    	int compareVal = t2._2.compareTo(t1._2); // Anzahl (absteigend)
    	if (compareVal == 0) { // IDs (aufsteigend)
			compareVal = t2._1.compareTo(t1._1) * (-1); // umgedreht, weil aufsteigend
		}
        return compareVal;
    }
}
