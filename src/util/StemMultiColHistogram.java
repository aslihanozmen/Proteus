package util;

import java.util.List;

public class StemMultiColHistogram extends Histogram<List<String>> {

    public boolean containsKey(List<String> key) {
        return getCountsUnsorted().containsKey(StemMultiColMap.stemList(key));
    }

    @Override
    public Double getScoreOf(List<String> key) {
        return super.getScoreOf(StemMultiColMap.stemList(key));
    }

    @Override
    public void increment(List<String> key, Double score) {
        super.increment(StemMultiColMap.stemList(key), score);
    }

    @Override
    public void increment(List<String> key) {
        super.increment(StemMultiColMap.stemList(key));
    }


    @Override
    public void setScoreOf(List<String> key, double score) {
        super.setScoreOf(StemMultiColMap.stemList(key), score);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (Pair<List<String>, Double> entry : getCountsSorted()) {
            sb.append(" (["
                    + StringUtil.join(entry.key, ", ")
                    + "]=>" + entry.value + ")");
        }

        sb.replace(0, 1, "{");
        sb.append("}");
        return super.toString();
    }

}
