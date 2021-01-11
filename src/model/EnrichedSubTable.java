package model;

import java.util.List;

import util.StemMultiColHistogram;
import util.StemMultiColMap;

public class EnrichedSubTable {

    private String tableId;
    private int[] xColumns;
    private int[] zColumns;
    private StemMultiColMap<StemMultiColHistogram> xToZMapping = new StemMultiColMap<>();
    private Table xzTable;

    private String columnHeader;

    public EnrichedSubTable(String tableID2, int[] xColumns, int[] zColumns, Table table) {
        this.setTableId(tableID2);
        this.setxColumns(xColumns);
        this.setzColumns(zColumns);
        this.setXzTable(table);
        setColumnHeader(tableID2);
        for (int i : zColumns) {
            setColumnHeader("_" + i);
        }

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

    public void setxToZMapping(StemMultiColMap<StemMultiColHistogram> xToZMapping) {
        this.xToZMapping = xToZMapping;
    }

    public void appendMapping(List<String> x, List<String> z) {
        xToZMapping.put(x, new StemMultiColHistogram());
        xToZMapping.get(x).increment(z);
    }


    public Table getXzTable() {
        return xzTable;
    }

    public void setXzTable(Table xzTable) {
        this.xzTable = xzTable;
    }

    public String getColumnHeader() {
        return columnHeader;
    }

    public void setColumnHeader(String columnHeader) {
        this.columnHeader = columnHeader;
    }

}
