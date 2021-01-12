package query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

import com.vertica.jdbc.VerticaConnection;

import util.StemHistogram;
import util.StemMap;
import util.MatchCase;

public class VerticaQuerier extends AbstractVerticaQuerier implements TableQuerier {
    public VerticaQuerier(VerticaConnection con) {
        this(con, Policy.ALL, -1);
    }

    public VerticaQuerier(VerticaConnection con, Policy policy, int maxExamples) {
        super(con, policy, maxExamples);
    }

    @Override
    public ArrayList<MatchCase> findTables(
            StemMap<StemMap<HashSet<MatchCase>>> keyToImages,
            StemMap<StemHistogram> knownExamples, int examplesPerQuery) {


        if (examplesPerQuery > knownExamples.size()) {
            for (String k : knownExamples.keySet()) {
                for (String v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                    System.out.println(k + " -> " + v);
                }
            }
            throw new IllegalStateException("|known examples| = " + knownExamples.size()
                    + ", tau = " + examplesPerQuery);
        }

        StemMap<StemHistogram> selectedExamples = null;

        try {
            selectedExamples = selectExamples(knownExamples);
        } catch (SQLException e) {
            e.printStackTrace();
        }


        ArrayList<String> xs = new ArrayList<>();
        ArrayList<String> ys = new ArrayList<>();
        for (String x : selectedExamples.keySet()) {
            xs.add(x);

            for (String y : selectedExamples.get(x).getCountsUnsorted().keySet()) {
                ys.add(y);
            }
        }


        return buildAndExecuteQuery(xs, ys, examplesPerQuery);
    }

}
