package testat02.APriori;

import org.apache.commons.collections.CollectionUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ItemSet implements java.io.Serializable {

    private HashSet<String> items;

    ItemSet() {
        items = new HashSet<String>();
    }

    ItemSet(ItemSet copySet) {
        items = new HashSet<String>();
        add(copySet);
    }

    ItemSet(ItemSet copySet1, ItemSet copySet2) {
        items = new HashSet<String>();
        add(copySet1);
        add(copySet2);
    }

    ItemSet(String string) {
        items = new HashSet<String>();
        items.add(string);
    }

    ItemSet(List<String> list) {
        items = new HashSet<String>();
        items.addAll(list);
    }

    HashSet<String> getItems() {
        return items;
    }

    void add(ItemSet itemSet) {
        items.addAll(itemSet.getItems());
    }

    private void add(String str) {
        items.add(str);
    }

    List<ItemSet> getPossibleCombinations(ItemSet set) {
        if (items.size() != set.getItems().size() || this.equals(set)) {
            return new ArrayList<ItemSet>();
        }
        int simAmount = items.size() - 1;
        List<ItemSet> combinations = new ArrayList<>();
        List<String> inBoth = getAllDoubles(set);
        if (inBoth.size() == simAmount) {
            ItemSet comb = new ItemSet(this, set);
            combinations.add(comb);
            return combinations;
        }
        return new ArrayList<ItemSet>();
    }

    private List<String> getAllDoubles(ItemSet set) {
        List<String> list = new ArrayList<>();
        for (String str : items) {
            if (set.getItems().contains(str)) {
                list.add(str);
            }
        }
        return list;
    }

    private void remove(String str) {
        items.remove(str);
    }

    boolean containsAllElements(ItemSet itemSet) {
        return items.containsAll(itemSet.getItems());
    }

    List<Rule> generateRules() {
        List<Rule> rules = new ArrayList<Rule>();
        for (String str : items) {
            ItemSet body = new ItemSet(this);
            body.remove(str);
            Rule rule = new Rule(body, new ItemSet(str));
            rules.add(rule);
        }
        return rules;
    }

    public static void main(String[] args) {
        ItemSet test2 = new ItemSet();
        test2.add("x");
        test2.add("b");
        test2.add("d");
        ItemSet test3 = new ItemSet();
        test3.add("x");
        test3.add("b");
        test3.add("c");
        System.out.println(test2.getPossibleCombinations(test3));
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
