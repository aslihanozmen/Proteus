package util;

public class StemHistogram extends Histogram<String> {
    public boolean containsKey(String key) {
        return getCountsUnsorted().containsKey(StemMap.getStem(key));
    }

    @Override
    public Double getScoreOf(String value) {
        return super.getScoreOf(StemMap.getStem(value));
    }

    @Override
    public void increment(String value, Double score) {
        super.increment(StemMap.getStem(value), score);
    }

    @Override
    public void increment(String value) {
        super.increment(StemMap.getStem(value));
    }


    @Override
    public void setScoreOf(String value, double score) {
        super.setScoreOf(StemMap.getStem(value), score);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (Pair<String, Double> entry : getCountsSorted()) {
            sb.append(" (" + entry.key + "=>" + entry.value + ")");
        }

        sb.replace(0, 1, "{");
        sb.append("}");
        return super.toString();
    }

    @Override
    public void remove(String key) {
        super.remove(StemMap.getStem(key));
    }
}
