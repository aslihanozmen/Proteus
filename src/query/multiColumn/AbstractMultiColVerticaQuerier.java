package query.multiColumn;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import com.google.common.collect.Lists;
import org.magicwerk.brownies.collections.GapList;
import util.Histogram;
import util.MultiColMatchCase;
import util.Pair;
import util.StemMultiColHistogram;
import util.StemMultiColMap;
import util.StringUtil;

import com.vertica.jdbc.VerticaConnection;

public class AbstractMultiColVerticaQuerier {

    public enum Policy {
        ALL,
        MOST_FREQX,
        MOST_FREQ_PAIR,
        LEAST_FREQX,
        LEAST_FREQ_PAIR,
        MOST_CERTAIN_PAIRS,
        MOST_CERTAIN_XS,
        CERTAIN_ONLY,
        CERTAIN_FILL_WITH_MOST_CERTAIN
    }

    protected Policy policy = Policy.ALL;
    protected int maxExamples = -1;
    protected int maxExamplesPerX = -1;
    protected VerticaConnection con;
    protected boolean useUnionTables = false;

    public AbstractMultiColVerticaQuerier(VerticaConnection con, Policy policy, int maxExamples, int maxExamplesPerX) {
        this.con = con;
        this.policy = policy;
        this.maxExamples = maxExamples;
        this.maxExamplesPerX = maxExamplesPerX;
    }

    public AbstractMultiColVerticaQuerier(VerticaConnection con, Policy policy, int maxExamples, int maxExamplesPerX, boolean useUnionTables) {
        this.con = con;
        this.policy = policy;
        this.maxExamples = maxExamples;
        this.maxExamplesPerX = maxExamplesPerX;
        this.useUnionTables = useUnionTables;
    }

    protected StemMultiColMap<StemMultiColHistogram> selectExamples(StemMultiColMap<StemMultiColHistogram> knownExamples) throws SQLException {
        StemMultiColMap<StemMultiColHistogram> selectedExamples = new StemMultiColMap<>();

        if (policy == Policy.ALL || knownExamples.size() <= maxExamples) {
            return knownExamples;
        } else {
            HashSet<List<String>> selectedKeys = new HashSet<>();

            if (policy == Policy.MOST_CERTAIN_PAIRS) {
                selectedExamples = selectMostCertainPairs(knownExamples);

            } else if (policy == Policy.MOST_CERTAIN_XS) {
                Histogram<Pair<List<String>, List<String>>> pairToScore = new Histogram<Pair<List<String>, List<String>>>();

                for (List<String> k : knownExamples.keySet()) {
                    for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                        pairToScore.increment(new Pair<List<String>, List<String>>(k, v), knownExamples.get(k).getScoreOf(v));
                    }
                }

                List<Pair<Pair<List<String>, List<String>>, Double>> sortedScores
                        = pairToScore.getCountsSorted();
                //				System.out.println(sortedScores);
                for (int i = 0; selectedKeys.size() < maxExamples; i++) {
                    Pair<List<String>, List<String>> entry = sortedScores.get(i).key;
                    List<String> k = entry.key;

                    selectedKeys.add(k);
                }


                for (List<String> k : selectedKeys) {
                    List<Pair<List<String>, Double>> sortedValsForK = knownExamples.get(k).getCountsSorted();
                    selectedExamples.put(k, new StemMultiColHistogram());
                    for (int i = 0; i < maxExamplesPerX && i < sortedValsForK.size(); i++) {
                        selectedExamples.get(k).increment(sortedValsForK.get(i).key, sortedValsForK.get(i).value);
                    }
                }

            } else if (policy == Policy.CERTAIN_ONLY) {
                selectedExamples = selectAllCertainPairs(knownExamples);
            } else if (policy == Policy.CERTAIN_FILL_WITH_MOST_CERTAIN) {

                selectedExamples = selectAllCertainPairs(knownExamples);
                if (selectedExamples.size() < maxExamples) {
                    selectedExamples = selectMostCertainPairs(knownExamples);
                }

            } else {
                throw new IllegalStateException("ERROR: UNKNOWN EXAMPLE SELECTION POLICY");
            }

            return selectedExamples;
        }
    }

    /**
     * @param knownExamples
     * @return Map with examples that have a perfect score of 1.0
     */
    private StemMultiColMap<StemMultiColHistogram> selectAllCertainPairs(
            StemMultiColMap<StemMultiColHistogram> knownExamples) {
        StemMultiColMap<StemMultiColHistogram> selectedExamples = new StemMultiColMap<>();
        for (List<String> k : knownExamples.keySet()) {
            for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                if (knownExamples.get(k).getScoreOf(v).equals(1.0)) {
                    if (!selectedExamples.containsKey(k)) {
                        selectedExamples.put(k, new StemMultiColHistogram());
                    }

                    selectedExamples.get(k).increment(v, 1.0);
                }
            }
        }
        return selectedExamples;
    }

    /**
     * @param knownExamples
     * @return Map with examples with the highest score
     */
    private StemMultiColMap<StemMultiColHistogram> selectMostCertainPairs(
            StemMultiColMap<StemMultiColHistogram> knownExamples) {
        StemMultiColMap<StemMultiColHistogram> selectedExamples = new StemMultiColMap<StemMultiColHistogram>();

        Histogram<Pair<List<String>, List<String>>> pairToScore = new Histogram<Pair<List<String>, List<String>>>();

        for (List<String> k : knownExamples.keySet()) {
            for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                pairToScore.increment(new Pair<>(k, v), knownExamples.get(k).getScoreOf(v));
            }
        }

        List<Pair<Pair<List<String>, List<String>>, Double>> sortedScores
                = pairToScore.getCountsSorted();
        for (int i = 0; i < maxExamples && i < sortedScores.size(); i++) {
            Pair<List<String>, List<String>> entry = sortedScores.get(i).key;
            List<String> k = entry.key;
            if (!selectedExamples.containsKey(k)) {
                selectedExamples.put(k, new StemMultiColHistogram());
            }
            selectedExamples.get(k).increment(entry.value, sortedScores.get(i).value);
        }

        return selectedExamples;
    }

    /**
     * @param xs
     * @param ys
     * @param examplesPerQuery
     * @return
     */
    protected ArrayList<MultiColMatchCase> buildAndExecuteQuery(Collection<String>[] xs, Collection<String>[] ys, int examplesPerQuery) {
        ArrayList<MultiColMatchCase> results = new ArrayList<MultiColMatchCase>();
        StringBuilder sb = new StringBuilder("");

        if (useUnionTables) {
            sb = buildQueryUnionTables(xs, ys, examplesPerQuery);
        } else {
            sb = buildQuery(xs, ys, examplesPerQuery);
        }

        sb.append(" limit 1000");
        //System.out.println("Started vertica query");
        //long startTime = System.currentTimeMillis();
        try {
            PreparedStatement ps = con.prepareStatement(sb.toString());
            int count = 0;
            for (int j = 0; j < xs.length; j++) {
                for (Iterator<String> iterator = xs[j].iterator(); iterator.hasNext(); ) {
                    count++;
                    ps.setObject(count, iterator.next());
                }
            }
            for (int j = 0; j < ys.length; j++) {
                for (Iterator<String> iterator = ys[j].iterator(); iterator.hasNext(); ) {
                    count++;
                    ps.setObject(count, iterator.next());
                }
            }

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                int[] colsFrom = new int[xs.length];
                int[] colsTo = new int[ys.length];

                for (int ci = 0; ci < xs.length; ci++) //Needed?
                {
                    colsFrom[ci] = rs.getInt(2 + ci);
                }
                for (int ci = 0; ci < ys.length; ci++) {
                    colsTo[ci] = rs.getInt(2 + xs.length + ci);
                }
                results.add(new MultiColMatchCase(rs.getString(1), colsFrom, colsTo));
            }

            rs.close();
            ps.close();

//            startTime = System.currentTimeMillis();
//            double secs = (System.currentTimeMillis() - startTime) * 1.0 / 1000;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return results;
    }

    private StringBuilder buildQueryUnionTables(Collection<String>[] xs, Collection<String>[] ys, int examplesPerQuery) {
        StringBuilder builder = new StringBuilder("SELECT col1.unionid");

        for (int i = 0; i < xs.length; i++) {
            builder.append(", col" + (i + 1) + ".colid");
        }
        for (int i = 0; i < ys.length; i++) {
            builder.append(", col" + (xs.length + i + 1) + ".colid");
        }
        builder.append(" FROM ");
        for (int ci = 0; ci < xs.length; ci++) {
            builder.append(" (SELECT unionid, colid FROM main_tokenized_union WHERE tokenized IN (");

            String[] qmarks = new String[xs[ci].size()];
            for (int i = 0; i < qmarks.length; i++) {
                qmarks[i] = "?";
            }

            builder.append(StringUtil.join(qmarks, ","));
            builder.append(")");
            builder.append(" GROUP BY unionid, colid HAVING COUNT(DISTINCT tokenized) >= " + (int) Math.min(xs.length, examplesPerQuery)
                    + ") AS col" + (ci + 1) + ",");
        }

        for (int ci = 0; ci < ys.length; ci++) {
            builder.append("(SELECT unionid, colid FROM main_tokenized_union WHERE tokenized IN (");

            String[] qmarks = new String[ys[ci].size()];
            for (int i = 0; i < qmarks.length; i++) {
                qmarks[i] = "?";
            }

            builder.append(StringUtil.join(qmarks, ","));
            builder.append(")");
            builder.append(" GROUP BY unionid, colid HAVING COUNT(DISTINCT tokenized) >= " + examplesPerQuery
                    + ") AS col" + (xs.length + ci + 1) + ",");
        }
        builder.setCharAt(builder.length() - 1, ' '); //remove last comma
        builder.append(" WHERE 1=1");

        for (int ci = 1; ci < xs.length; ci++) {
            builder.append(" AND col1.unionid = col" + (ci + 1) + ".unionid");
        }
        for (int ci = 0; ci < ys.length; ci++) {
            builder.append(" AND col1.unionid = col" + (xs.length + ci + 1) + ".unionid");
        }
        for (int ci = 0; ci < xs.length; ci++) {
            for (int cj = ci + 1; cj < xs.length; cj++) {
                builder.append(" AND col" + (ci + 1)
                        + ".colid <> col" + (cj + 1) + ".colid");

            }
            for (int cj = 0; cj < ys.length; cj++) {
                builder.append(" AND col" + (ci + 1)
                        + ".colid <> col" + (xs.length + cj + 1) + ".colid");
            }

        }

        return builder;
    }

    public List<String> countWords(String input) {
        if (input == null || input.isEmpty()) {
            return null;
        }
        return Lists.newArrayList(input.split("\\s+"));
    }

// TODO - It is for substring and based on word extraction

//    private StringBuilder buildQuery(Collection<String>[] xs, Collection<String>[] ys, int examplesPerQuery) {
//        List<String> inputsQuery = getWordsFromExamples(xs);
//        List<String> outputsQuery = getWordsFromExamples(ys);
//
//        StringBuilder sb = new StringBuilder("SELECT col1.tableid");
//
//        for (int i = 0; i < xs.length; i++) {
//            sb.append(", col" + (i + 1) + ".colid");
//        }
//        for (int i = 0; i < ys.length; i++) {
//            sb.append(", col" + (xs.length + i + 1) + ".colid");
//        }
//        sb.append(" FROM ");
//        for (int ci = 0; ci < xs.length; ci++) {
//            sb.append(" (SELECT tableid, colid FROM main_tokenized_union WHERE tokenized IN (");
//
//            String[] qmarks = new String[xs[ci].size()];
//            for (int i = 0; i < qmarks.length; i++) {
//                qmarks[i] = "?";
//            }
//
//            sb.append(StringUtil.join(qmarks, ","));
//            sb.append(") ");
//
////            String[] qmarksForLIKE = new String[xs[ci].size()];
////            Object[] xValues = xs[ci].toArray();
////            for (int i = 0; i < qmarksForLIKE.length; i++) {
////
////                qmarksForLIKE[i] = "OR tokenized LIKE '%" + xValues[i].toString() + "%' ";
////            }
////            sb.append(StringUtil.join(qmarksForLIKE, " "));
//
//            String[] qmarksForMatch = new String[inputsQuery.size()];
//            Object[] xValuesForMatch = inputsQuery.toArray();
//            for (int i = 0; i < qmarksForMatch.length; i++) {
//
//                qmarksForMatch[i] = "OR tokenized LIKE '%" + xValuesForMatch[i].toString() + "%'";
//            }
//            sb.append(StringUtil.join(qmarksForMatch, " "));
//            sb.append(" GROUP BY tableid, colid HAVING COUNT(DISTINCT tokenized) >= " + (int) Math.min(xs.length, examplesPerQuery)
//                    + ") AS col" + (ci + 1) + ",");
//        }
//
//        for (int ci = 0; ci < ys.length; ci++) {
//            sb.append("(SELECT tableid, colid FROM main_tokenized_union WHERE tokenized IN (");
//
//            String[] qmarks = new String[ys[ci].size()];
//            for (int i = 0; i < qmarks.length; i++) {
//                qmarks[i] = "?";
//            }
//            sb.append(StringUtil.join(qmarks, ","));
//            sb.append(") ");
//
////            String[] qmarksForLIKE =  new String[ys[ci].size()];
////            Object[] yValues = ys[ci].toArray();
////            for (int i = 0; i < qmarksForLIKE.length; i++) {
////                qmarksForLIKE[i] = "OR tokenized LIKE '%" + yValues[i].toString() + "%' ";
////            }
////            sb.append(StringUtil.join(qmarksForLIKE, " "));
//
//            String[] qmarksForMatch =  new String[outputsQuery.size()];
//            Object[] yValuesForMatch = outputsQuery.toArray();
//            for (int i = 0; i < qmarksForMatch.length; i++) {
//                qmarksForMatch[i] = "OR tokenized LIKE '%" + yValuesForMatch[i].toString() + "%'";
//            }
//            sb.append(StringUtil.join(qmarksForMatch, " "));
//
//
//            sb.append(" GROUP BY tableid, colid HAVING COUNT(DISTINCT tokenized) >= " + examplesPerQuery
//                    + ") AS col" + (xs.length + ci + 1) + ",");
//        }
//
//        sb.setCharAt(sb.length() - 1, ' '); //remove last comma
//
//        sb.append(" WHERE 1=1");
//
//        for (int ci = 1; ci < xs.length; ci++) {
//            sb.append(" AND col1.tableid = col" + (ci + 1) + ".tableid");
//        }
//        for (int ci = 0; ci < ys.length; ci++) {
//            sb.append(" AND col1.tableid = col" + (xs.length + ci + 1) + ".tableid");
//        }
//        for (int ci = 0; ci < xs.length; ci++) {
//            for (int cj = ci + 1; cj < xs.length; cj++) {
//                sb.append(" AND col" + (ci + 1)
//                        + ".colid <> col" + (cj + 1) + ".colid");
//
//            }
//            for (int cj = 0; cj < ys.length; cj++) {
//                sb.append(" AND col" + (ci + 1)
//                        + ".colid <> col" + (xs.length + cj + 1) + ".colid");
//            }
//
//        }
//        return sb;
//    }

    private StringBuilder buildQuery(Collection<String>[] xs, Collection<String>[] ys, int examplesPerQuery) {
        StringBuilder sb = new StringBuilder("SELECT col1.tableid");

        for (int i = 0; i < xs.length; i++) {
            sb.append(", col").append(i + 1).append(".colid");
        }
        for (int i = 0; i < ys.length; i++) {
            sb.append(", col").append(xs.length + i + 1).append(".colid");
        }

        sb.append(", col1.rowid");

        sb.append(" FROM ");
        for (int ci = 0; ci < xs.length; ci++) {
            sb.append(" (SELECT tableid, colid, rowid FROM main_tokenized_union WHERE tokenized IN (");

            String[] qmarks = new String[xs[ci].size()];
            Arrays.fill(qmarks, "?");

            sb.append(StringUtil.join(qmarks, ","));
            sb.append(")");
            sb.append(" GROUP BY tableid, colid, rowid HAVING COUNT(DISTINCT tokenized) >= ")
                    .append(Math.min(xs.length, examplesPerQuery)).append(") AS col").append(ci + 1).append(",");
        }

        for (int ci = 0; ci < ys.length; ci++) {
            sb.append("(SELECT tableid, colid FROM main_tokenized_union WHERE tokenized IN (");

            String[] qmarks = new String[ys[ci].size()];
            Arrays.fill(qmarks, "?");

            sb.append(StringUtil.join(qmarks, ","));
            sb.append(")");
            sb.append(" GROUP BY tableid, colid HAVING COUNT(DISTINCT tokenized) >= ").append(examplesPerQuery)
                    .append(") AS col").append(xs.length + ci + 1).append(",");
        }

        sb.setCharAt(sb.length() - 1, ' '); //remove last comma

        sb.append(" WHERE 1=1");

        for (int ci = 1; ci < xs.length; ci++) {
            sb.append(" AND col1.tableid = col").append(ci + 1).append(".tableid");
        }
        for (int ci = 0; ci < ys.length; ci++) {
            sb.append(" AND col1.tableid = col").append(xs.length + ci + 1).append(".tableid");
        }
        for (int ci = 0; ci < xs.length; ci++) {
            for (int cj = ci + 1; cj < xs.length; cj++) {
                sb.append(" AND col").append(ci + 1).append(".colid <> col").append(cj + 1).append(".colid");

            }
            for (int cj = 0; cj < ys.length; cj++) {
                sb.append(" AND col").append(ci + 1).append(".colid <> col").append(xs.length + cj + 1).append(".colid");
            }

        }
        return sb;
    }

    private List<String> getWordsFromExamples(Collection<String>[] xs) {
        List<String> queryVariables = new GapList<>();
        for(Collection<String> word : xs) {
            String[] words = new String[0];
            for(String string : word){
                 queryVariables.addAll(countWords(string));
            }
            Collections.addAll(queryVariables, words);
        }
        return queryVariables;
    }
}