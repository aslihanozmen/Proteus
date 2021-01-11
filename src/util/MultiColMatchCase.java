package util;

import java.util.ArrayList;
import java.util.Arrays;

public class MultiColMatchCase {
    public String tableID;
    public int[] col1;
    public int[] col2;
    public int row1;
    private ArrayList<String> context;

    public MultiColMatchCase(String tableID, int[] col1, int[] col2) {
        this.tableID = tableID;
        this.col1 = col1;
        this.col2 = col2;
        context = new ArrayList<>();
    }

    public MultiColMatchCase(String tableID, int[] col1, int[] col2, int row1) {
        this.tableID = tableID;
        this.col1 = col1;
        this.col2 = col2;
        this.row1 = row1;
    }

    public int getRow1() {
        return row1;
    }

    public void setRow1(int row1) {
        this.row1 = row1;
    }

    public void setContext(ArrayList<String> terms){
        this.context = terms;
    }

    public ArrayList<String> getContext() {
        return this.context;
    }

    @Override
    public String toString() {
        return "(" + tableID + ", " + Arrays.toString(col1) + ", " + Arrays.toString(col2) +  ", " + row1 + ")";
    }


    public String getTableID() {
        return tableID;
    }


    public void setTableID(String tableID) {
        this.tableID = tableID;
    }


    public int[] getCol1() {
        return col1;
    }


    public void setCol1(int[] col1) {
        this.col1 = col1;
    }


    public int[] getCol2() {
        return col2;
    }


    public void setCol2(int[] col2) {
        this.col2 = col2;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(col1);
        result = prime * result + Arrays.hashCode(col2);
        result = prime * result + ((tableID == null) ? 0 : tableID.hashCode());
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
        MultiColMatchCase other = (MultiColMatchCase) obj;
        if (!Arrays.equals(col1, other.col1))
            return false;
        if (!Arrays.equals(col2, other.col2))
            return false;
        if (tableID == null) {
            if (other.tableID != null)
                return false;
        } else if (!tableID.equals(other.tableID))
            return false;
        return true;
    }


}
