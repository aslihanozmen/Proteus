package query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import util.Histogram;
import util.MatchCase;
import util.Pair;
import util.StemHistogram;
import util.StemMap;
import util.StringUtil;

import com.vertica.jdbc.VerticaConnection;

public class AbstractVerticaQuerier {

    public enum Policy {
        ALL,
        MOST_FREQX,
        MOST_FREQ_PAIR,
        LEAST_FREQX,
        LEAST_FREQ_PAIR,
        MOST_CERTAIN
    }

    protected Policy policy = Policy.ALL;
    protected int maxExamples = -1;
    protected VerticaConnection con;

    public AbstractVerticaQuerier(VerticaConnection con, Policy policy, int maxExamples) {
        this.con = con;
        this.policy = policy;
        this.maxExamples = maxExamples;
    }

    /**
     * Select the most frequent examples from the knowExamples. Number of returned examples depends on policy chosen.
     *
     * @param knownExamples
     * @return Map with selected examples
     * @throws SQLException
     */
    protected StemMap<StemHistogram> selectExamples(StemMap<StemHistogram> knownExamples) throws SQLException {
        if (policy == Policy.ALL || knownExamples.size() <= maxExamples) {
            return knownExamples;
        } else {
            HashSet<String> selectedKeys = new HashSet<>();

            if (policy == Policy.MOST_FREQX) {

                String[] qmarks = new String[knownExamples.size()];
                for (int j = 0; j < qmarks.length; j++) {
                    qmarks[j] = "?";
                }
                String qmarksStr = StringUtil.join(qmarks, ",");

                PreparedStatement ps = con.prepareStatement(
                        "SELECT tokenized, freq FROM (" +
                                "SELECT tokenized, COUNT(DISTINCT tableid || '#' || colid) AS freq"
                                + " FROM tokenized_to_col "
                                + " WHERE tokenized IN (" + qmarksStr + ")"
                                + " GROUP BY tokenized"
                                + " ORDER BY tokenized"
                                + ") AS tmp"
                                + " ORDER BY freq DESC, tokenized"
                                + " LIMIT " + maxExamples);

                String[] keys = knownExamples.keySet().toArray(new String[0]);

                for (int j = 0; j < qmarks.length; j++) {
                    ps.setString(j + 1, keys[j]);
                }

                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    selectedKeys.add(rs.getString("tokenized"));
                }

                rs.close();
                ps.close();
            } else if (policy == Policy.MOST_FREQ_PAIR) {
                HashSet<Pair<String, String>> freqPairs = new HashSet<Pair<String, String>>();
                int tupsSize = 0;
                for (String k : knownExamples.keySet()) {
                    tupsSize += knownExamples.get(k).getCountsUnsorted().size();
                }


                String[] qmarks1 = new String[knownExamples.size()];
                for (int j = 0; j < qmarks1.length; j++) {
                    qmarks1[j] = "?";
                }
                String qmarksStr1 = StringUtil.join(qmarks1, ",");


                String[] qmarks2 = new String[tupsSize];
                for (int j = 0; j < qmarks2.length; j++) {
                    qmarks2[j] = "?";
                }
                String qmarksStr2 = StringUtil.join(qmarks2, ",");


                PreparedStatement ps = con.prepareStatement(
                        "SELECT tok1, tok2, freq FROM ("
                                + " SELECT t1.tokenized AS tok1, t2.tokenized AS tok2, COUNT(DISTINCT t1.tableid) AS freq FROM "
                                + " (SELECT tokenized, tableid, rowid FROM tokenized_to_col  where tokenized IN ( "
                                + qmarksStr1 + ") ORDER BY tokenized, tableid) AS t1,"
                                + " (SELECT tokenized, tableid, rowid FROM tokenized_to_col where tokenized IN ( "
                                + qmarksStr2 + ") ORDER BY tokenized, tableid) AS t2"
                                + " WHERE t1.tableid = t2.tableid"
                                + " AND t1.rowid = t2.rowid"
                                + " GROUP BY t1.tokenized, t2.tokenized"
                                + " ORDER BY t1.tokenized, t2.tokenized) AS tmp"
                                + " ORDER BY freq DESC, tok1, tok2");

                String[] keys = knownExamples.keySet().toArray(new String[0]);

                for (int j = 0; j < qmarks1.length; j++) {
                    ps.setString(j + 1, keys[j]);
                }
                int q = qmarks1.length + 1;
                for (String k : knownExamples.keySet()) {
                    for (String v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                        ps.setString(q, v);
                        q++;
                    }
                }

                int foundPairs = 0;
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String k = rs.getString("tok1");
                    String v = rs.getString("tok2");
                    if (knownExamples.get(k).getCountsUnsorted().containsKey(v)) {
                        if (selectedKeys.contains(k)) {
                            continue;
                        }
                        freqPairs.add(new Pair<String, String>(k, v));
                        foundPairs++;

                        selectedKeys.add(k);

                        if (foundPairs == maxExamples) {
                            break;
                        }
                    }
                }

                rs.close();
                ps.close();
            } else if (policy == Policy.LEAST_FREQX) {
                String[] qmarks = new String[knownExamples.size()];
                for (int j = 0; j < qmarks.length; j++) {
                    qmarks[j] = "?";
                }
                String qmarksStr = StringUtil.join(qmarks, ",");

                PreparedStatement ps = con.prepareStatement(
                        "SELECT tokenized, freq FROM (" +
                                "SELECT tokenized, COUNT(DISTINCT tableid || '#' || colid) AS freq"
                                + " FROM tokenized_to_col "
                                + " WHERE tokenized IN (" + qmarksStr + ")"
                                + " GROUP BY tokenized"
                                + " ORDER BY tokenized"
                                + ") AS tmp"
                                + " ORDER BY freq, tokenized"
                                + " LIMIT " + maxExamples);

                String[] keys = knownExamples.keySet().toArray(new String[0]);

                for (int j = 0; j < qmarks.length; j++) {
                    ps.setString(j + 1, keys[j]);
                }

                //long start2 = System.currentTimeMillis();

                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    selectedKeys.add(rs.getString("tokenized"));
                }

                rs.close();
                ps.close();
            } else if (policy == Policy.LEAST_FREQ_PAIR) {
                HashSet<Pair<String, String>> freqPairs = new HashSet<Pair<String, String>>();
                int tupsSize = 0;
                for (String k : knownExamples.keySet()) {
                    tupsSize += knownExamples.get(k).getCountsUnsorted().size();
                }


                String[] qmarks1 = new String[knownExamples.size()];
                for (int j = 0; j < qmarks1.length; j++) {
                    qmarks1[j] = "?";
                }
                String qmarksStr1 = StringUtil.join(qmarks1, ",");


                String[] qmarks2 = new String[tupsSize];
                for (int j = 0; j < qmarks2.length; j++) {
                    qmarks2[j] = "?";
                }
                String qmarksStr2 = StringUtil.join(qmarks2, ",");


                PreparedStatement ps = con.prepareStatement(
                        "SELECT tok1, tok2, freq FROM ("
                                + " SELECT t1.tokenized AS tok1, t2.tokenized AS tok2, COUNT(DISTINCT t1.tableid) AS freq FROM "
                                + " (SELECT tokenized, tableid, rowid FROM tokenized_to_col  where tokenized IN ( "
                                + qmarksStr1 + ") ORDER BY tokenized, tableid) AS t1,"
                                + " (SELECT tokenized, tableid, rowid FROM tokenized_to_col where tokenized IN ( "
                                + qmarksStr2 + ") ORDER BY tokenized, tableid) AS t2"
                                + " WHERE t1.tableid = t2.tableid"
                                + " AND t1.rowid = t2.rowid"
                                + " GROUP BY t1.tokenized, t2.tokenized"
                                + " ORDER BY t1.tokenized, t2.tokenized) AS tmp"
                                + " ORDER BY freq, tok1, tok2");

                String[] keys = knownExamples.keySet().toArray(new String[0]);

                for (int j = 0; j < qmarks1.length; j++) {
                    ps.setString(j + 1, keys[j]);
                }
                int q = qmarks1.length + 1;
                for (String k : knownExamples.keySet()) {
                    for (String v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                        ps.setString(q, v);
                        q++;
                    }
                }

                int foundPairs = 0;
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String k = rs.getString("tok1");
                    String v = rs.getString("tok2");
                    if (knownExamples.get(k).getCountsUnsorted().containsKey(v)) {
                        if (selectedKeys.contains(k)) {
                            continue;
                        }
                        freqPairs.add(new Pair<String, String>(k, v));
                        foundPairs++;

                        selectedKeys.add(k);

                        if (foundPairs == maxExamples) {
                            break;
                        }
                    }
                }

                rs.close();
                ps.close();

            } else if (policy == Policy.MOST_CERTAIN) {
                Histogram<Pair<String, String>> pairToScore = new Histogram<Pair<String, String>>();

                for (String k : knownExamples.keySet()) {
                    for (String v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                        pairToScore.increment(new Pair<String, String>(k, v), knownExamples.get(k).getScoreOf(v));
                    }
                }

                List<Pair<Pair<String, String>, Double>> sortedScores = pairToScore.getCountsSorted();
                for (int i = 0; selectedKeys.size() < maxExamples; i++) {
                    Pair<String, String> entry = sortedScores.get(i).key;
                    String k = entry.key;
                    selectedKeys.add(k);
                }


            } else {
                throw new IllegalStateException("ERROR: UNKNOWN EXAMPLE SELECTION POLICY");
            }


            StemMap<StemHistogram> selectedExamples = new StemMap<>();
            for (String k : selectedKeys) {
                selectedExamples.put(k, knownExamples.get(k));
            }

            return selectedExamples;
        }// else not return all
    }

    public Histogram<String> getCooccFreq(String x, Collection<String> vals) throws SQLException {
        Histogram<String> scores = new Histogram<>();
        for (String s : vals) {
            scores.increment(s, (double) getCooccFreq(x, s));
        }

        return scores;
    }

    /**
     * Returns the column co-occurrence score
     *
     * @param x1
     * @param x2
     * @throws SQLException
     */
    public int getCooccFreq(String x1, String x2) throws SQLException {
        PreparedStatement ps = con.prepareStatement(
                "SELECT tok2, freq FROM (" +
                        "SELECT t2.tokenized AS tok2, COUNT(DISTINCT t2.tableid || '#' || t2.colid) AS freq"
                        + " FROM tokenized_to_col t1, tokenized_to_col t2 "
                        + " WHERE t1.tokenized = ? AND t2.tokenized = ?"
                        + " AND t1.tableid = t2.tableid AND t1.colid = t2.colid"
                        + " GROUP BY t1.tokenized, t2.tokenized"
                        + ") AS tmp"
                        + " ORDER BY freq DESC, tok2");

        ps.setString(1, x1);
        ps.setString(2, x2);

        ResultSet rs = ps.executeQuery();

        int coocc = 0;
        if (rs.next()) {
            coocc = rs.getInt("freq");
        }

        rs.close();
        ps.close();

        return coocc;
    }

    /**
     * @param xs
     * @param ys
     * @param examplesPerQuery
     * @return ArrayList containing all found MatchCases(tableid, colid1, colid2)
     */
    protected ArrayList<MatchCase> buildAndExecuteQuery(ArrayList<String> xs, ArrayList<String> ys, int examplesPerQuery) {
        ArrayList<MatchCase> results = new ArrayList<MatchCase>();

        String[] qmarks = new String[xs.size()];
        for (int i = 0; i < qmarks.length; i++) {
            qmarks[i] = "?";
        }
        String[] qmarks2 = new String[ys.size()];
        for (int j = 0; j < qmarks2.length; j++) {
            qmarks2[j] = "?";
        }

        long startTime = System.currentTimeMillis();
        try {
            PreparedStatement ps = con.prepareStatement("SELECT col1.tableid, col1.colid, col2.colid " +
                    "FROM (SELECT tableid, colid FROM tokenized_to_col WHERE tokenized IN (" + StringUtil.join(qmarks, ",") + ") " +
                    "GROUP BY tableid, colid HAVING COUNT(DISTINCT tokenized) >= " + examplesPerQuery + ") AS col1," +
                    "(SELECT tableid, colid FROM tokenized_to_col WHERE tokenized IN (" + StringUtil.join(qmarks2, ",") + ") " +
                    "GROUP BY tableid, colid HAVING COUNT(DISTINCT tokenized) >= " + examplesPerQuery + ") AS col2 " +
                    "WHERE col1.tableid = col2.tableid AND col1.colid <> col2.colid");
            for (int j = 0; j < xs.size(); j++) {
                ps.setObject(j + 1, xs.get(j));
            }
            for (int j = 0; j < ys.size(); j++) {
                ps.setObject(xs.size() + j + 1, ys.get(j));
            }


            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                results.add(new MatchCase(rs.getString(1), rs.getInt(2), rs.getInt(3)));
            }

            double secs = (System.currentTimeMillis() - startTime) * 1.0 / 1000;
            System.out.println("Finished vertica query..time = " + secs + "\t" + results.size() + " triples found.");

            rs.close();
            ps.close();

            startTime = System.currentTimeMillis();
            secs = (System.currentTimeMillis() - startTime) * 1.0 / 1000;
            System.out.println("Finished loading queried tables..time = " + secs);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return results;
    }
}

	