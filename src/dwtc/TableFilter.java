package dwtc;

import model.Table;
import model.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TableFilter {

    public enum DATATYPE {
        STRING,
        DATE,
        NUMBER,
        CURRENCY
    }

    private static final double COLON_FRACTION_THRESHOLD = 0.75;

    private static double getFractionEndingInColon(List<String> cells) {
        double fractionEndingInColon = 0;
        int size = 0;
        for (String cell : cells) {
            if (cell == null) {
                continue;
            }

            int colonIndex = cell.lastIndexOf(":");
            if (colonIndex > -1 && !cell.substring(colonIndex).matches(".*\\w+.*")) {
                fractionEndingInColon += 1;
            }
            size++;
        }
        return fractionEndingInColon / size;
    }

    /**
     *
     * @param table
     * @return An int that indicates if table has header and if it is transposed
     * 0: No header detected
     * 1: Header found
     * 2: Has Header and is transposed
     */
    public static int hasHeader(Table table) {
        List<String> cells = new ArrayList<>();
        for (int i = 0; i < table.getNumCols(); i++) {
            cells.add(table.getRow(0).getCell(i).getValue());
        }

        double fractionEndingInColon = getFractionEndingInColon(cells);
        if (fractionEndingInColon >= COLON_FRACTION_THRESHOLD) {
            return 1;
        }

        //Check if column has colons
        fractionEndingInColon = getFractionEndingInColon(cells);
        if (fractionEndingInColon >= COLON_FRACTION_THRESHOLD) {
            return 2;
        }

        //Has header row?
        ArrayList<String> valsInRow = new ArrayList<>();
        for (int i = 0; i < table.getNumCols(); i++) {
            valsInRow.add(table.getRow(0).getCell(i).getValue());
        }
        DATATYPE firstRowType = findTypeOfList(valsInRow);

        if (firstRowType == DATATYPE.STRING) {
            for (int i = 0; i < table.getNumCols(); i++) {
                valsInRow = new ArrayList<>();
                for (int j = 0; j < table.getNumRows(); j++) {
                    valsInRow.add(table.getRow(j).getCell(i).getValue());
                }
                DATATYPE type = findTypeOfList(valsInRow);

                if (type != null && firstRowType != type) {
                    return 1;
                }
            } //end for table.getNumCols()
        }

        // Check if it has a header column
        ArrayList<String> valsInCol = new ArrayList<String>();
        for (Tuple tuple : table.getTuples()) {
            valsInCol.add(tuple.getCell(0).getValue());
        }
        DATATYPE firstColType = findTypeOfList(valsInCol);

        if (firstColType == DATATYPE.STRING) {
            for (Tuple tuple : table.getTuples()) {
                valsInCol = new ArrayList<String>();
                for (int i = 1; i < table.getNumCols(); i++) {
                    valsInCol.add(tuple.getCell(i).getValue());
                }
                DATATYPE type = findTypeOfList(valsInCol);

                if (type != null && firstColType != type) {
                    return 2;
                }
            }
        }

        return 0;
    }

    public static DATATYPE findTypeOfList(Collection<String> values) {
        boolean allNulls = true;
        for (String v : values) {
            if (v != null) {
                allNulls = false;
                break;
            }
        }
        if (allNulls) {
            return null;
        }

        if (allMatchRegex(values, "^(\\s|&nbsp;)*-?(\\s|&nbsp;)*([0-9]+(\\.[0-9]+)?|[0]*\\.[0-9]+)((\\s|&nbsp;)?%)?(\\s|&nbsp;)*$")) {
            return DATATYPE.NUMBER;
        } else if (allMatchRegex(values, "^(\\s|&nbsp;)*[0-3]?[0-9](-|/|.)[0-3]?[0-9](-|/|.)([0-9]{2})?[0-9]{2}(\\s|&nbsp;)*$")) {
            return DATATYPE.DATE;
        } else if (allMatchRegex(values, "^(\\s|&nbsp;)*(\\$|\\u20ac|EUR|USD|CAD|C\\$)( )?\\d+ | \\d+( )?(\\$|\\u20ac|EUR|USD|CAD|C\\$)(\\s|&nbsp;)*$")) {
            return DATATYPE.CURRENCY;
        }
        return DATATYPE.STRING;
    }

    public static boolean allMatchRegex(Collection<String> values, String regex) {
        for (String v : values) {
            if (v != null && !v.matches(regex)) {
                return false;
            }
        }

        return true;
    }
}
