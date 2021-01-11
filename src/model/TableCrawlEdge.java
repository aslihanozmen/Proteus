package model;

public class TableCrawlEdge {
    public TableCrawlNode source;
    public TableCrawlNode dest;

    public double score;

    /**
     * Matching score
     **/

    public TableCrawlEdge(TableCrawlNode source, TableCrawlNode dest, double matchScore) {
        this.source = source;
        this.dest = dest;
        this.score = matchScore;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dest == null) ? 0 : dest.hashCode());
        result = prime * result + ((source == null) ? 0 : source.hashCode());
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
        TableCrawlEdge other = (TableCrawlEdge) obj;
        if (dest == null) {
            if (other.dest != null)
                return false;
        } else if (!dest.equals(other.dest))
            return false;
        if (source == null) {
            if (other.source != null)
                return false;
        } else if (!source.equals(other.source))
            return false;
        return true;
    }


}
