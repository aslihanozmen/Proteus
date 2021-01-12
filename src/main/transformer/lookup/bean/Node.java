package main.transformer.lookup.bean;

import java.util.Set;

public class Node implements Comparable<Node>, Cloneable {

    private String table;
    private Set<String> sourceField;
    private String destField;

    public Node(String table, Set<String> sourceField, String destField) {
        this.table = table;
        this.sourceField = sourceField;
        this.destField = destField;
    }

    public Set<String> getSourceField() {
        return sourceField;
    }

    public CandidateKey getDestKey() {
        return new CandidateKey(table, destField);
    }

    @Override
    public int compareTo(Node other) {
        return this.sourceField.size() - other.sourceField.size();
    }
}
