package query.multiColumn;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.lucene.queryparser.classic.ParseException;

import util.MultiColMatchCase;
import util.StemMultiColHistogram;
import util.StemMultiColMap;

import com.vertica.jdbc.VerticaConnection;

public class MultiColVerticaQuerier extends AbstractMultiColVerticaQuerier implements MultiColTableQuerier {

    public MultiColVerticaQuerier(VerticaConnection con) {
        this(con, Policy.ALL, -1, -1);
    }

    public MultiColVerticaQuerier(VerticaConnection con, Policy policy, int maxExamples, int maxExamplesPerX) {
        super(con, policy, maxExamples, maxExamplesPerX);
    }

    public MultiColVerticaQuerier(VerticaConnection con, Policy policy, int maxExamples, int maxExamplesPerX, boolean useUnionTables) {
        super(con, policy, maxExamples, maxExamplesPerX, useUnionTables);
    }


    @Override
    public ArrayList<MultiColMatchCase> findTables(
            StemMultiColMap<StemMultiColHistogram> knownExamples, int examplesPerQuery) {
        if (examplesPerQuery > knownExamples.size() && examplesPerQuery > knownExamples.values().iterator().next().getCountsUnsorted().size()) {
            for (List<String> k : knownExamples.keySet()) {
                for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                    System.out.println(k + " -> " + v);
                }
            }
            throw new IllegalStateException("|known examples| = " + knownExamples.size()
                    + ", tau = " + examplesPerQuery);
        }


        StemMultiColMap<StemMultiColHistogram> selectedExamples = null;

        try {
            selectedExamples = selectExamples(knownExamples);
        } catch (SQLException e) {
            e.printStackTrace();
        }


        HashSet<String>[] xs = new HashSet[knownExamples.keySet().iterator().next().size()];
        HashSet<String>[] ys = new HashSet[knownExamples.values().iterator().next().getCountsUnsorted().keySet().iterator().next().size()];

        for (int i = 0; i < xs.length; i++) {
            xs[i] = new HashSet<String>();
        }
        for (int i = 0; i < ys.length; i++) {
            ys[i] = new HashSet<String>();
        }

        for (List<String> x : selectedExamples.keySet()) {
            for (int i = 0; i < xs.length; i++) {
                xs[i].add(x.get(i));
            }

            for (List<String> y : selectedExamples.get(x).getCountsUnsorted().keySet()) {
                for (int i = 0; i < ys.length; i++) {
                    ys[i].add(y.get(i));
                }
            }
        }

        return buildAndExecuteQuery(xs, ys, examplesPerQuery);
    }

    public ArrayList<MultiColMatchCase> similarFindTables(
            List<String> inputs, List<String> outputs,  int examplesPerQuery) {
        if (examplesPerQuery > inputs.size() && examplesPerQuery > outputs.size()) {
            throw new IllegalStateException("|inputs from examples| = " + inputs.size()
                    + ", tau = " + examplesPerQuery);
        }

        HashSet<String>[] xs = new HashSet[inputs.size()];
        HashSet<String>[] ys = new HashSet[inputs.size()];

        for (int i = 0; i < xs.length; i++) {
            xs[i] = new HashSet<>();
        }
        for (int i = 0; i < ys.length; i++) {
            ys[i] = new HashSet<>();
        }

            for (int i = 0; i < xs.length; i++) {
                xs[i].add(inputs.get(i));
            }

            for (int i = 0; i < ys.length; i++) {
                    ys[i].add(outputs.get(i));
                }

        return buildAndExecuteQuery(xs, ys, examplesPerQuery);
    }


    /**
     * Finds tables with columns (x-columns) satisfying the tau criteria
     *
     * @param xs
     * @param examplesPerQuery
     * @return
     * @throws IOException
     * @throws ParseException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public ArrayList<MultiColMatchCase> findTables(
            Collection<String>[] xs,
            int examplesPerQuery) throws IOException, ParseException,
            InterruptedException, ExecutionException {
        return buildAndExecuteQuery(xs, new Collection[0], examplesPerQuery);
    }
}
