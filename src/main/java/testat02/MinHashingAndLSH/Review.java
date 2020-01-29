package testat02.MinHashingAndLSH;

import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class Review implements java.io.Serializable {

    private int id;
    private Collection<String> shingles;

    Review(String review, int shingleSize) {
        shingles = new HashSet<>();
        int commaIndex = review.indexOf(",");
        this.id = Integer.parseInt(review.substring(1, commaIndex - 1));
        review = review.substring(commaIndex + 2, review.length());
        for (int i = 0; i < review.length() - 1; i++) { // Shingles bilden
            int endIndex = i + shingleSize;
            if (endIndex >= review.length()) {
                endIndex = review.length() - 1;
            }
            shingles.add(review.substring(i, endIndex));
        }
    }

    int getID() {
        return id;
    }

    Collection<String> getShingles() {
        return shingles;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(id);
        stringBuilder.append(": ");
        stringBuilder.append(shingles.toString());
        return stringBuilder.toString();
    }

}
