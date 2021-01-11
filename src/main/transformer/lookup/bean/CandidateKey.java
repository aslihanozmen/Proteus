package main.transformer.lookup.bean;

import java.util.Objects;

public class CandidateKey {

    //denoted T in the article
    private String table;
    //denoted C in the article
    private String column;

    //Candidate Key
    public CandidateKey(String table, String column) {
        this.table = table;
        this.column = column;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CandidateKey key = (CandidateKey) o;
        return Objects.equals(table, key.table) &&
                Objects.equals(column, key.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, column);
    }

    @Override
    public String toString() {
        return column;
    }
}
