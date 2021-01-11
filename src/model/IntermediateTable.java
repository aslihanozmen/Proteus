package model;

import java.util.ArrayList;
import java.util.List;

import util.MultiColMatchCase;
import util.Pair;
import util.StemMultiColHistogram;
import util.StemMultiColMap;

public class IntermediateTable {

    private String tableId;
    private int[] xColumns;
    private int[] zColumns;
    private StemMultiColMap<StemMultiColHistogram> xToZMapping = new StemMultiColMap<>();
    private StemMultiColMap<StemMultiColHistogram> zToYMapping = new StemMultiColMap<>();
    private List<Pair<Table, MultiColMatchCase>> zyTables = new ArrayList<>();
    private Table xzTable;

    public IntermediateTable(String tableID2, int[] xColumns, int[] zColumns, Table table) {
        this.setTableId(tableID2);
        this.setxColumns(xColumns);
        this.setzColumns(zColumns);
        this.setXzTable(table);
    }

    public int[] getxColumns() {
        return xColumns;
    }

    public void setxColumns(int[] xColumns) {
        this.xColumns = xColumns;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public int[] getzColumns() {
        return zColumns;
    }

    public void setzColumns(int[] zColumns) {
        this.zColumns = zColumns;
    }

    public StemMultiColMap<StemMultiColHistogram> getxToZMapping() {
        return xToZMapping;
    }

    public StemMultiColMap<StemMultiColHistogram> getZToYMapping() {
        return zToYMapping;
    }

    public void setxToZMapping(StemMultiColMap<StemMultiColHistogram> xToZMapping) {
        this.xToZMapping = xToZMapping;
    }

    public void appendMapping(List<String> x, List<String> z, List<String> y) {
        xToZMapping.put(x, new StemMultiColHistogram());
        xToZMapping.get(x).increment(z);
        zToYMapping.put(z, new StemMultiColHistogram());
        zToYMapping.get(z).increment(y);
    }

    public void addZYTable(Table zyTable, MultiColMatchCase tableTriple) {
        zyTables.add(new Pair<>(zyTable, tableTriple));

    }

    public List<Pair<Table, MultiColMatchCase>> getzyTables() {

        return zyTables;
    }

    public Table getXzTable() {
        return xzTable;
    }

    public void setXzTable(Table xzTable) {
        this.xzTable = xzTable;
    }

}
