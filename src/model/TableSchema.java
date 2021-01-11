package model;

import java.io.Serializable;
import java.util.Arrays;


/**
 * This class should make it easier for users to express constraints using column names
 *
 * @author xchu
 */
public class TableSchema implements Serializable {

    private String[] columnNames;    //Map column names to column positions, columns positions are determined during initialization phase

    private String[] columnTypes;    //Map column names to column Types

    public TableSchema(String[] line) {
        init(line);
    }

    public String[] getColumnHead() {
        return columnNames;
    }

    public void init(String[] columns) {
        columnNames = new String[columns.length];
        columnTypes = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {

            columnNames[i] = columns[i].replaceAll("&nbsp;", " ").replaceAll("\\s+", " ").trim();
            columnTypes[i] = "String";
        }
    }


    public int NametoPosition(String name) {
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equalsIgnoreCase(name))
                return i;
        }

        System.out.println("Invalid Column Name " + name);
        return 0; //there is no such column
    }

    public String NametoType(String name) {
        int pos = NametoPosition(name);

        return columnTypes[pos];

    }

    public String positionToName(int pos) {
        return columnNames[pos];
    }

    public String positionToType(int pos) {
        return columnTypes[pos];
    }

    public String[] getColumnNames() {
        return columnNames;
    }


    public String[] getColumnNames(int[] colIndices) {
        String[] headers = new String[colIndices.length];
        for (int i = 0; i < colIndices.length; i++) {
            headers[i] = columnNames[colIndices[i]];
        }
        return headers;
    }


    public void addColumn(String colName, String colType) {
        columnNames = Arrays.copyOf(columnNames, columnNames.length + 1);
        columnTypes = Arrays.copyOf(columnTypes, columnTypes.length + 1);

        columnNames[columnNames.length - 1] = colName;
        columnTypes[columnTypes.length - 1] = colType;
    }


    public void removeColumn(String colName) {
        removeColumn(NametoPosition(colName));
    }

    public void removeColumn(int colIdx) {
        String[] colNames2 = new String[columnNames.length - 1];
        String[] colTypes2 = new String[columnNames.length - 1];

        for (int i = 0; i < colIdx; i++) {
            colNames2[i] = columnNames[i];
            colTypes2[i] = columnTypes[i];
        }
        for (int i = colIdx + 1; i < columnNames.length; i++) {
            colNames2[i - 1] = columnNames[i];
            colTypes2[i - 1] = columnTypes[i];
        }

        columnNames = colNames2;
        columnTypes = colTypes2;
    }
}
