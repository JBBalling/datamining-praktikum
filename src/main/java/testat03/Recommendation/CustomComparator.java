package testat03.Recommendation;

import scala.Tuple2;

import java.util.Comparator;

// Comparator um Tuple zu sortieren
public class CustomComparator implements Comparator<Tuple2<Integer, Integer>> {
    @Override
    public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
        int compareVal = t2._2.compareTo(t1._2); // Anzahl (absteigend)
        if (compareVal == 0) { // IDs (aufsteigend)
            compareVal = t2._1.compareTo(t1._1) * (-1); // umgedreht, weil aufsteigend
        }
        return compareVal;
    }
}
