package model.columnMapping;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import main.WTTransformerMultiColSets;
import model.Table;
import util.HungarianAlgorithm;
import util.Pair;

public abstract class AbstractColumnMatcher {
    /**
     * Easy and universal solution for transitive matching formulae
     */
    //protected HashMap<String, String> standardizedForm = new HashMap<String, String>();

    /**
     * Pair is alphabetically ordered.
     */
    protected HashSet<Pair<String, String>> cache = new HashSet<>();

    /**
     * Should two nulls match or not. Default: no match
     */
    protected boolean nullsMatch = true;

    public AbstractColumnMatcher() {
        super();
    }

    public int getCoverage(Table inputTable, int anchorCol, Table webTable, int webCol) {
        int count = 0;

        List<String> webRows = webTable.getCellsinCol(webCol);
        List<String> inputRows = inputTable.getCellsinCol(anchorCol);

        for (String webRow : webRows) {
            if (webRow == null) {
                continue;
            }
            //TODO: for now we are assuming distinct values
            for (String inputRow : inputRows) {
                if (match(webRow, inputRow)) {
                    count++;
                    break;
                }
            }
        }

        return count;
    }

    /**
     * Finds the column in the webtable that corresponds to the input column
     *
     * @param inputTable
     * @param anchorCol
     * @param webTable
     * @return <index of the webcolumn, coverage> or NULL if none
     */
    public Pair<Integer, Integer> mapColumn(Table inputTable, int anchorCol, Table webTable) {
        int maxCoverage = 0;
        int bestCol = -1;

        for (int i = 0; i < webTable.getNumCols(); i++) {
            int coverage = getCoverage(inputTable, anchorCol, webTable, i);

            if (coverage > maxCoverage) {
                maxCoverage = coverage;
                bestCol = i;
            }
        }

        if (maxCoverage >= WTTransformerMultiColSets.COVERAGE_THRESHOLD) {
            return new Pair<Integer, Integer>(bestCol, maxCoverage);
        } else {
            return null;
        }
    }

    private boolean checkMapping(Table inputTable, int colTo, Table t,
                                 int col) {
        int cov = getCoverage(inputTable, colTo, t, col);
        return cov > WTTransformerMultiColSets.COVERAGE_THRESHOLD /* && cov * 1.0 / t.getNumRows() > SUPPORT_THRESHOLD*/;

    }


    /**
     * 1-1 column mapping. Instance based (?)
     *
     * @param table1
     * @param table2
     * @return Column mapping. Key is index of column in table 1, Value is the mapped column from table 2
     */
    public HashMap<Integer, Integer> colMap(Table table1, Table table2) {
        //The hungarian algorithm
        double[][] scores = new double[table1.getNumCols()][table2.getNumCols()];
        //Mappings
        for (int col1 = 0; col1 < table1.getNumCols(); col1++) {
            //mapColumn(table1, col1, table2);
            for (int col2 = 0; col2 < table2.getNumCols(); col2++) {
                double matchScore = getCoverage(table1, col1, table2, col2);

                if (matchScore > WTTransformerMultiColSets.COVERAGE_THRESHOLD) {
                    scores[col1][col2] = matchScore;
                }

            }

        }

        HungarianAlgorithm hungarian = new HungarianAlgorithm(scores);
        int[] assignments = hungarian.execute();

        HashMap<Integer, Integer> matching = new HashMap<Integer, Integer>();

        for (int i = 0; i < assignments.length; i++) {
            if (assignments[i] != -1 && scores[i][assignments[i]] > WTTransformerMultiColSets.COVERAGE_THRESHOLD) {
                matching.put(i, assignments[i]);
            }
        }

        return matching;
    }


    public static void joinScore(String table1, int col1, String table2, int col2) {
        throw new UnsupportedOperationException();
    }


    public boolean match(String s1, String s2) {
        if (s1 == null && s2 == null) {
            return nullsMatch;
        } else if (s1 == null && s2 != null) {
            return false;
        } else if (s1 != null && s2 == null) {
            return false;
        } else {
            Pair<String, String> cacheEntry = null;
            if (s1.compareTo(s2) == 0) {
                return true;
            } else if (s1.compareTo(s2) < 0) {
                cacheEntry = new Pair<String, String>(s1, s2);
            } else if (s1.compareTo(s2) > 0) {
                cacheEntry = new Pair<String, String>(s2, s1);
            }

            if (cache.contains(cacheEntry)) {
                return true;
            } else {
                boolean match = computeDoMatch(s1, s2);
                if (match) {
                    cache.add(cacheEntry);
                }
                return match;
            }
        }
    }

    public abstract boolean computeDoMatch(String s1, String s2);

}