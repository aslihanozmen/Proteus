package model;

import java.io.Serializable;

import util.Similarity;


public class Cell implements Serializable {

    private String type;    //The data type of the cell
    private String value;    //The value of the cell stored in string form


    public Cell(String type, String value) {
        this.type = type;
        this.value = value;
    }

    public Cell(Cell cell) {
        this.type = cell.type;
        this.value = cell.value;
    }

    public String getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String newValue) {
        this.value = newValue;
    }

    public boolean isSameValue(Cell cell) {
        assert (type.equalsIgnoreCase(cell.getType()));
        return value.equals(cell.getValue());
    }

    public boolean isSameValue(String val) {
        return value.equals(val);

    }

    /**
     * Check if two cells are similar according to the Levenshtein Distance. Only works for Strings.
     *
     * @param cell
     * @return True if cell values are similar, false otherwise
     */
    public boolean isSimilarValue(Cell cell) {
        assert (type.equalsIgnoreCase(cell.getType()));

        String type1 = type;
        String type2 = cell.getType();
        if (!type1.equals(type2)) {
            System.out.println("These two cells are not of the same type, thus aren't comparable!");
            return false;
        } else {
            if (type2.equalsIgnoreCase("String")) {
                return Similarity.levenshteinDistance(value, cell.getValue());
            } else {
                System.out.println("These two cells must be of String type");
                return false;
            }
        }
    }

    /**
     * Check if cell is similar to the given value according to the Levenshtein Distance.
     *
     * @param val
     * @return True if cell value is similar to provided String, false otherwise
     */
    public boolean isSimilarValue(String val) {
        String type1 = type;
        if (type1.equalsIgnoreCase("String")) {
            return Similarity.levenshteinDistance(value, val);
        } else {
            System.out.println("These two cells must be of String type");
            return false;
        }
    }

    /**
     * Check if the this cell's value is greater than the argument cell's value. Works for String, Integer and Double
     *
     * @param cell
     * @return True if this cell's value is greater than the value of the arguments cell.
     */
    public boolean greaterThan(Cell cell) {
        assert (type.equalsIgnoreCase(cell.getType()));
        String type1 = type;
        if (type1.equalsIgnoreCase("Integer")) {
            int value1 = Integer.valueOf(value);
            int value2 = Integer.valueOf(cell.getValue());
            return (value1 > value2);
        } else if (type1.equalsIgnoreCase("String")) {
            String value1 = value;
            String value2 = cell.getValue();
            int com = value1.compareTo(value2);
            return (com > 0);
        } else if (type1.equalsIgnoreCase("Double")) {
            double value1 = Double.valueOf(value);
            double value2 = Double.valueOf(cell.getValue());
            return (value1 > value2);
        } else {
            System.out.println("Unsupported Type For now");
            return false;
        }
    }

    /**
     * Check if the this cell's value is greater than the argument cell's value.
     *
     * @param val
     * @return True if this cell's value is greater than the value of the arguments cell.
     */
    public boolean greaterThan(String val) {
        String type1 = type;
        if (type1.equalsIgnoreCase("Integer")) {
            int value1 = Integer.valueOf(value);
            int value2 = Integer.valueOf(val);
            return (value1 > value2);
        } else if (type1.equalsIgnoreCase("String")) {
            String value1 = value;
            String value2 = val;
            int com = value1.compareTo(value2);
            return (com > 0);
        } else if (type1.equalsIgnoreCase("Double")) {
            double value1 = Double.valueOf(value);
            double value2 = Double.valueOf(val);
            return (value1 > value2);
        } else {
            System.out.println("Unsupported Type For now");
            return false;
        }
    }

    /**
     * @return the cell's value
     */
    public String toString() {
        return value;
    }
}
