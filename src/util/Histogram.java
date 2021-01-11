package util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;


public class Histogram<K> {

    protected HashMap<K, Double> counts = new HashMap<>();
    private double totalCounts = 0;

    public Double getScoreOf(K value) {
        return counts.get(value);
    }


    public void increment(K value, Double score) {
        if (counts.containsKey(value)) {
            counts.put(value, counts.get(value) + score);
        } else {
            counts.put(value, score);
        }

        totalCounts += score;
    }


    public void increment(K value) {
        increment(value, 1.0);
    }


    @SuppressWarnings("unchecked")
    public HashMap<K, Double> getCountsUnsorted() {
        return (HashMap<K, Double>) counts.clone();
    }

    @SuppressWarnings("unchecked")
    public List<Pair<K, Double>> getCountsSorted() {

        HashMap<K, Double> clone = (HashMap<K, Double>) counts.clone();

        Entry<K, Double>[] arr = clone.entrySet().toArray((Entry<K, Double>[]) new Entry[0]);

        Arrays.sort(arr, new Comparator<Entry<K, Double>>() {
            public int compare(Entry<K, Double> o1, Entry<K, Double> o2) {
                return -Double.compare(o1.getValue(), o2.getValue());
            }
        });

        List<Pair<K, Double>> sortedCounts = new ArrayList<>();

        for (Entry<K, Double> entry : arr) {
            sortedCounts.add(new Pair<K, Double>(entry.getKey(), entry.getValue()));
        }

        return sortedCounts;
    }

    public void setScoreOf(K key, double score) {
        counts.put(key, score);

        totalCounts = 0;
        for (double v : counts.values()) {
            totalCounts += v;
        }

    }

    public double getTotalCount() {
        return totalCounts;
    }


    public void remove(K key) {
        double score = counts.get(key);
        counts.remove(key);
        totalCounts -= score;
    }

}
