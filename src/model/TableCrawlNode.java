package model;

public class TableCrawlNode {
    public String table;
    public int col1;
    public int col2;

    /**
     * This one estimates how many tuples are covered
     * TODO: what if more than 1 incoming edge? ESTIMATE: MAX
     */
    public int tuplesCovered;

    /**
     * With different incoming values, should always pick the minimum
     */
    public int distFromRoot;

    public TableCrawlNode(String table, int col1, int col2) {
        this.table = table;
        this.col1 = col1;
        this.col2 = col2;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + col1;
        result = prime * result + col2;
        result = prime * result + ((table == null) ? 0 : table.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TableCrawlNode other = (TableCrawlNode) obj;
        if (col1 != other.col1)
            return false;
        if (col2 != other.col2)
            return false;
        if (table == null) {
            if (other.table != null)
                return false;
        } else if (!table.equals(other.table))
            return false;
        return true;
    }


}
