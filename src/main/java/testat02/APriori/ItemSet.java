package testat02.APriori;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ItemSet implements java.io.Serializable {

    private ArrayList<String> items;

    public ItemSet() {
        items = new ArrayList<String>();
    }

    public ItemSet(ItemSet copySet) {
        items = new ArrayList<String>();
        add(copySet);
    }

    public ItemSet(ItemSet copySet1, ItemSet copySet2) {
        items = new ArrayList<String>();
        add(copySet1);
        add(copySet2);
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

    public ArrayList<ItemSet> getPossibleCombinations(ItemSet set) {
        if (items.size() != set.getItems().size()) {
            return new ArrayList<ItemSet>();
        }
        int simAmount = items.size() - 1;
        ArrayList<ItemSet> combinations = new ArrayList<>();
        ArrayList<String> inBoth = getAllDoubles(set);
        ArrayList<String> notInBoth = new ArrayList<>();
        if (inBoth.size() == simAmount) {
            for (String str : items) {
                if (!inBoth.contains(str)) {
                    ItemSet comb = new ItemSet(inBoth);
                    comb.add(str);
                    combinations.add(comb);
                }
            }
            for (String str : set.getItems()) {
                if (!inBoth.contains(str)) {
                    ItemSet comb = new ItemSet(inBoth);
                    comb.add(str);
                    combinations.add(comb);
                }
            }
            return combinations;
        } else if (inBoth.size() > simAmount) {
            combinations.add(new ItemSet(inBoth));
            return combinations;
        }
        return new ArrayList<ItemSet>();
    }

    public ArrayList<String> getAllDoubles(ItemSet set) {
        ArrayList<String> list = new ArrayList<>();
        for (String str : items) {
            if (set.getItems().contains(str)) {
                list.add(str);
            }
        }
        return list;
    }

    public void remove(String str) {
        items.remove(str);
    }

    public boolean containsAllElements(ItemSet itemSet) {
        return items.containsAll(itemSet.getItems());
    }

    public ArrayList<Rule> generateRules() {
        ArrayList<Rule> rules = new ArrayList<Rule>();
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
        test2.add("a");
        test2.add("b");
        System.out.println(test2.generateRules());
        ItemSet test3 = new ItemSet();
        test3.add("a");
        test3.add("b");
        test3.add("c");
        System.out.println(test3.generateRules());
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
