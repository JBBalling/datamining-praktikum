package testat03.Recommendation;

import java.util.HashSet;
import java.util.Set;

public class User implements java.io.Serializable {

    private int userID;
    private Set<Integer> friends;

    User(String input) {
        String[] array = input.split("\t");
        userID = Integer.parseInt(array[0]);
        friends = new HashSet<>();
        if (array.length > 1) {
            for (String str : array[1].split(",")) {
                friends.add(Integer.parseInt(str));
            }
        }
    }

    public int getUserID() {
        return userID;
    }

    public Set<Integer> getFriends() {
        return friends;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(userID);
        stringBuilder.append(": ");
        stringBuilder.append(friends.toString());
        return stringBuilder.toString();
    }

}
