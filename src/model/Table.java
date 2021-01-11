package model;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

public class Table implements Serializable {

    public static final boolean WITH_HEADER_DEFAULT = true;

    //***********************************
    //* Fields **************************
    //***********************************
    private TableSchema schema;
    private int numRows;
    private int numCols;
    private List<Tuple> tuples = new ArrayList<Tuple>();
    private String tableName;

    public double confidence = 0.0;
    public int source = -1;
    //scaled to between 0 and 1
    public double openrank = 0.0;
    public String[] termSet = new String[]{};

    //***********************************
    //* Constructors ********************
    //***********************************
    public Table() {
        schema = new TableSchema(new String[0]);
    }

    public Table(Reader input) {
        this(input, Integer.MAX_VALUE, WITH_HEADER_DEFAULT);
    }

    public Table(String inputPath) {
        this(inputPath, Integer.MAX_VALUE, WITH_HEADER_DEFAULT);
    }

    public Table(String inputPath, boolean withHeader) {
        this(inputPath, Integer.MAX_VALUE, withHeader);
    }

    public Table(Reader input, boolean withHeader) {
        this(input, Integer.MAX_VALUE, withHeader);
    }

    public Table(String inputPath, int numRows, boolean withHeader) {
        this.numRows = numRows;

        initFromCSV(inputPath, withHeader);
    }

    public Table(String inputPath, int numRows, boolean withHeader, String tableName) {
        this.numRows = numRows;
        this.tableName = tableName;

        initFromCSV(inputPath, withHeader);
    }

    public Table(Reader input, int numRows, boolean withHeader) {
        this.numRows = numRows;

        initFromCSV(input, withHeader);
    }

    public Table(Table table, boolean withData) {
        this.schema = new TableSchema(table.schema.getColumnHead());
        this.numRows = 0;
        this.numCols = table.numCols;

        if (withData) {
            for (Tuple tuple : table.tuples) {
                addTuple(new Tuple(tuple));
            }
        }
    }

    //***********************************
    //* Methods *************************
    //***********************************

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    private boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

    private boolean isDouble(String s) {
        try {
            Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

    private void initFromCSV(String inputPath, boolean withHeader) {
        try {
            initFromCSV(new InputStreamReader(new FileInputStream(inputPath), "UTF-8"), withHeader);
        } catch (UnsupportedEncodingException | FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void initFromCSV(Reader input, boolean withHeader) {
        int temp = 0;
        try {
            CSVReader csvReader = new CSVReader(input, ',', '"');

            String[] colValues;
            while ((colValues = csvReader.readNext()) != null) {
                if (temp == 0) {

                    this.numCols = colValues.length;

                    if (withHeader) // First line is the columns information
                    {
                        schema = new TableSchema(colValues);
                    } else {
                        String[] defaultCols = new String[colValues.length];
                        for (int i = 0; i < defaultCols.length; i++) {
                            defaultCols[i] = "COLUMN" + i;
                        }

                        schema = new TableSchema(defaultCols);

                        Tuple tuple = new Tuple(colValues, schema, 0);
                        tuples.add(tuple);

                        temp++;
                    }
                    temp++;
                } else {
                    if (colValues.length != schema.getColumnNames().length) {
                        continue;
                    }

                    Tuple tuple = new Tuple(colValues, schema, temp - 1);
                    tuples.add(tuple);
                    temp++;
                    if (temp > numRows) {
                        break;
                    }
                }
            }
            numRows = tuples.size();
            Collections.sort(tuples);
            csvReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.err.println("File was not found!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TableSchema getColumnMapping() {
        return schema;
    }

    public boolean hasHeader() {
        for (int i = 0; i < getNumCols(); i++) {
            if (schema.getColumnNames()[i] == null || !schema.getColumnNames()[i].equals("COLUMN" + i)) {
                return true;
            }
        }

        return false;
    }

    public void setSchema(TableSchema schema) {
        this.schema = schema;
        this.numCols = schema.getColumnNames().length;
    }

    public int getNumRows() {
        return numRows;
    }

    public int getNumCols() {
        return numCols;
    }

    public List<Tuple> getTuples() {
        return tuples;
    }

    public Tuple getRow(int idx) {
        return tuples.get(idx);
    }

    public Tuple getTuple(int idx) {
        return tuples.get(idx);
    }

    public void removeTuples(Collection<Tuple> toBeRemoved) {
        tuples.removeAll(toBeRemoved);
        numRows = tuples.size();
    }

    public void removeTuple(Tuple toBeRemoved) {
        tuples.remove(toBeRemoved);
        numRows = tuples.size();
    }

    public void retainTuple(Set<Tuple> toBeRetained) {
        tuples.retainAll(toBeRetained);
        numRows = tuples.size();
    }

    public void insertTuples(Set<Tuple> toBeInserted) {
        tuples.addAll(toBeInserted);
        for (Tuple t : toBeInserted) {
            t.cm = this.schema;
        }
        numRows = tuples.size();
    }

    /**
     * Method used to append Tuples to the Table from another Table. Used to create Union Tables.
     * @param table - holds the tuples to append
     */
    public void concatTables(Table table) {
        tuples.addAll(table.getTuples());
        numRows += table.numRows;
    }

    /**
     * Get a cell for a row and col, both starting from 0
     *
     * @param row
     * @param col
     * @return
     */
    public Cell getCell(int row, int col) {
        return tuples.get(row).getCell(col);
    }

    public void setCell(int row, int col, String newValue) {
        tuples.get(row).getCell(col).setValue(newValue);
    }

    /**
     * Get all the constants in a particular column
     *
     * @param col
     * @return
     */
    public Set<String> getColumnValues(int col) {
        Set<String> colValues = new HashSet<String>();
        for (int i = 0; i < numRows; i++) {
            String value = tuples.get(i).getCell(col).getValue();
            colValues.add(value);
        }
        return colValues;
    }

    /**
     * @param col - Index of column (starting at 0)
     * @return
     */
    public List<String> projectColumn(int col) {
        List<String> colValues = new ArrayList<String>(numRows);
        for (int i = 0; i < numRows; i++) {
            String value = tuples.get(i).getCell(col).getValue();
            colValues.add(value);
        }
        return colValues;
    }

    /**
     * Get all the value with duplication in a particular column
     *
     * @param col
     * @return
     */
    public List<String> getCellsinCol(int col) {
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < numRows; i++) {
            String value = tuples.get(i).getCell(col).getValue();
            list.add(value);
        }
        return list;
    }

    public void addTuple(Tuple tuple) {
        tuples.add(tuple);
        tuple.cm = this.schema;
        numRows = tuples.size();
    }

    public void addColumn(String name) {
        String type = "String";
        schema.addColumn(name, type);

        for (Tuple t : tuples) {
            t.getCells().add(new Cell(type, null));
        }

        numCols = schema.getColumnNames().length;
    }

    public void writeToFile(String file) throws IOException {
        CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"), ',', '"');

        csvWriter.writeNext(schema.getColumnNames());

        String[] vals = new String[numCols];
        for (Tuple t : getTuples()) {
            for (int i = 0; i < vals.length; i++) {
                vals[i] = t.getCell(i).getValue();
            }
            csvWriter.writeNext(vals);
        }

        csvWriter.flush();
        csvWriter.close();
    }

    public void removeCol(int i) {
        schema.removeColumn(i);

        for (Tuple t : getTuples()) {
            t.getCells().remove(i);
        }

        numCols = schema.getColumnNames().length;
    }

    public void removeCols(List<Integer> colsToRemove) {
        Collections.sort(colsToRemove);

        for (int i = 0; i < colsToRemove.size(); i++) {
            removeCol(colsToRemove.get(i) - i); //Decrease the index more due to shifting (that's why we sorted)
        }

        numCols = schema.getColumnNames().length;
    }

    public void removeTupleAtIndex(int i) {
        tuples.remove(i);

        numRows--;
    }

    public void saveToCSVFile(String file) throws IOException {

        CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"), ',', '"');

//        String[] columnNames = schema.getColumnNames();
//        if(columnNames.length != 0) {
//            csvWriter.writeNext(columnNames);
//        } else {
//            StringBuilder sb = new StringBuilder();
//            for(int i = 0; i < numberCols; i++) {
//                sb.append("Column");
//                sb.append(i);
//                if(i < numberCols -1) {
//                    sb.append(",");
//                }
//            }
//            csvWriter.writeNext(sb.toString());
//        }

        for (Tuple tuple : tuples) {
            String[] tupVals = new String[tuple.getCells().size()];
            for (int i = 0; i < tupVals.length; i++) {
                tupVals[i] = tuple.getCell(i).getValue();
            }
            csvWriter.writeNext(tupVals);
        }
        csvWriter.close();
    }

    public Collection<String>[] getColumns(int[] colsFrom) {
        Collection<String>[] fromColumns = new ArrayList[colsFrom.length];
        for (int i = 0; i < fromColumns.length; i++) {
            fromColumns[i] = getColumn(colsFrom[i]);
        }
        return fromColumns;
    }

    public Collection<String> getColumn(int colsFrom) {
        Collection<String> fromColumn = new ArrayList<String>();
        for (int t = 0; t < tuples.size(); t++) {
            fromColumn.add(getCell(t, colsFrom).getValue());
        }
        return fromColumn;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();

        sb.append(schema.getColumnNames()[0]);
        for (int i = 1; i < schema.getColumnNames().length; i++) {
            sb.append(",\t" + schema.getColumnNames()[i]);
        }
        sb.append('\n');
        for (Tuple t : getTuples()) {
            sb.append(t.getCell(0).getValue());
            for (int i = 1; i < schema.getColumnNames().length; i++) {
                sb.append(",\t" + t.getCell(i).getValue());
            }
            sb.append('\n');
        }
        return sb.toString();
    }

}