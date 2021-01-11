package model;

import java.util.ArrayList;

public class TableCrawl {
    private ArrayList<TableCrawlNode> path = new ArrayList<TableCrawlNode>();

    public void addPathNode(String table, int col1, int col2) {
        path.add(new TableCrawlNode(table, col1, col2));
    }
}
