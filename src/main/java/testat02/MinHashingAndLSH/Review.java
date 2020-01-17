package testat02.MinHashingAndLSH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Review implements java.io.Serializable {

    private int id;
    private List<String> shingles;

    Review(String review, int shingleSize) {
        shingles = new ArrayList<>();
        int commaIndex = review.indexOf(",");
        this.id = Integer.parseInt(review.substring(1, commaIndex - 1));
        review = review.substring(commaIndex + 2, review.length());
        for (int i = 0; i < review.length() - 1; i++) {
            int endIndex = i + shingleSize;
            if (endIndex >= review.length()) {
                endIndex = review.length() - 1;
            }
            shingles.add(review.substring(i, endIndex));
        }
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
