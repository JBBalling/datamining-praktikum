package testat02.APriori;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class ItemSet {

    private ArrayList<String> items;

    public ItemSet() {
        items = new ArrayList<String>();
    }

    public ItemSet(Tuple2 tuple) {
        items = new ArrayList<String>();
        add((String)tuple._1);
        add((String)tuple._2);
    }

    public ArrayList getItems() {
        return items;
    }

    public void add(String item) {
        if (items.contains(item)) {
            return;
        }
        items.add(item);
    }

    public void add(String[] itemArr) {
        items.addAll(Arrays.asList(itemArr)
                .stream()
                .distinct()
                .collect(Collectors.toList())
        );
    }

    @Override
    public boolean equals(Object o) {
        if (!o.getClass().equals(this.getClass())) {
            return false;
        }
        ItemSet other = (ItemSet) o;
        return items.equals(other.getItems());
    }

    @Override
    public final int hashCode() {
        return items.hashCode();
    }

}
