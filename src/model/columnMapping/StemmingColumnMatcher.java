package model.columnMapping;

import java.util.List;

import util.StemMap;
import model.Table;

public class StemmingColumnMatcher extends AbstractColumnMatcher {
    /**
     * @param inputTable
     * @param anchorCol  (0-indexed)
     * @param webTable
     * @param webCol     (0-indexed)
     * @return The number of rows covered by the webCol
     */
    @Override
    public int getCoverage(Table inputTable, int anchorCol, Table webTable, int webCol) {
        int count = 0;

        List<String> webRows = webTable.getCellsinCol(webCol);
        List<String> inputRows = inputTable.getCellsinCol(anchorCol);

        //Some cleaning
        for (int i = 0; i < webRows.size(); i++) {
            String webRow = webRows.get(i);
            if (webRow != null) {
                webRow = webRow.toLowerCase();
                webRow = webRow.replaceAll("&nbsp;", " ");
                webRow = webRow.replaceAll("\\s+", " ");
                webRow = webRow.trim();

                webRows.set(i, webRow);
            }
        }
        for (int i = 0; i < inputRows.size(); i++) {
            String inputRow = inputRows.get(i);
            if (inputRow != null) {
                inputRow = inputRow.toLowerCase();
                inputRow = inputRow.replaceAll("&nbsp;", " ");
                inputRow = inputRow.replaceAll("\\s+", " ");
                inputRow = inputRow.trim();

                inputRows.set(i, inputRow);
            }
        }

        for (String webRow : webRows) {
            //TODO: for now we are assuming distinct values
            for (String inputRow : inputRows) {
                if (webRow != null && webRow.equalsIgnoreCase(inputRow)) {
                    count++;
                    break;
                }
            }
        }

        return count;
    }


    @Override
    public boolean match(String s1, String s2) {
        if (s1 != null && s1.length() == 0) {
            s1 = null;
        }
        if (s2 != null && s2.length() == 0) {
            s2 = null;
        }


        if (s1 == null && s2 == null) {
            return nullsMatch;
        } else if (s1 == null && s2 != null) {
            return false;
        } else if (s1 != null && s2 == null) {
            return false;
        } else {
            return StemMap.getStem(s1).equalsIgnoreCase(StemMap.getStem(s2));
        }
    }

    @Override
    public boolean computeDoMatch(String s1, String s2) {
        return StemMap.getStem(s1).equalsIgnoreCase(StemMap.getStem(s2));
    }
}
