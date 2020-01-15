package testat02.APriori;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ItemSet implements java.io.Serializable {

    private ArrayList<String> items;

    public ItemSet() {
        items = new ArrayList<String>();
    }

    public ItemSet(String string) {
        items = new ArrayList<String>();
        add(string);
    }

    public ItemSet(List<String> list) {
        items = new ArrayList<String>();
        items.addAll(list);
    }

    public ArrayList<String> getItems() {
        return items;
    }

    public void add(String item) {
        if (items.contains(item)) {
            return;
        }
        items.add(item);
    }

    public void addAll(ArrayList<String> list) {
        for (String string : list) {
            if (!items.contains(string)) {
                items.add(string);
            }
        }
    }

    public void add(ItemSet itemSet) {
        addAll(itemSet.getItems());
    }

    public boolean containsAllElements(ItemSet itemSet) {
        return items.containsAll(itemSet.getItems());
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        boolean start = true;
        for (String string : items) {
            if (start) {
                start = false;
            } else {
                stringBuilder.append(", ");
            }
            stringBuilder.append(string);
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
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
