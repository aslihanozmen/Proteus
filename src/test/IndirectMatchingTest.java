package test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import main.WTTransformerIndirect;
import main.WTTransformerIndirect.ConsolidationMethod;
import model.Table;
import model.Tuple;
import model.columnMapping.AbstractColumnMatcher;
import model.multiColumn.VerticaMultiColBatchTableLoader;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.Before;
import org.junit.Test;

import query.multiColumn.MultiColTableQuerier;
import query.multiColumn.MultiColVerticaQuerier;
import util.MultiColMatchCase;
import util.Pair;
import util.StemHistogram;
import util.StemMap;
import util.StemMultiColHistogram;
import util.StemMultiColMap;
import util.fd.tane.FunctionalDependency;
import util.fd.tane.TANEjava;

import com.vertica.jdbc.VerticaConnection;

public class IndirectMatchingTest {

    public static StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth = null;
    public static String exampleCountDir = null;
    public static int initialExampleCounts = 0;
    public String inputCSVName = null;

    private Properties verticaProperties = null;


    @Before
    public void init() {
        WTTransformerIndirect.originalOnly = true;
        try {
            verticaProperties.load(new FileInputStream("/vertica.properties"));
            //verticaProperties.load(GeneralTests.class.getResourceAsStream("/vertica.properties"));
        } catch (IOException e) {
//            e.printStackTrace();
            System.err.println("Properties file not found");
        }
        if (verticaProperties == null) {
            verticaProperties = new Properties();
            verticaProperties.setProperty("user", "user");
            verticaProperties.setProperty("password", "password");
            verticaProperties.setProperty("database", "database");
            verticaProperties.setProperty("database_host", "jdbc:vertica://localhost/");
        }

    }

    @Test
    public void testIndirectMatchingSets(String file, int[] colsFrom, int[] colsTo) throws Exception {

        inputCSVName = file;

        StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth = new StemMultiColMap<StemMultiColMap<HashSet<String>>>();
        HashMap<List<String>, List<Tuple>> keysToTuples = new HashMap<List<String>, List<Tuple>>();

        Table fullTable = new Table(file);


        for (Tuple tuple : fullTable.getTuples()) {
            List<String> k = tuple.getValuesOfCells(colsFrom);
            List<String> v = tuple.getValuesOfCells(colsTo);

            if (!groundTruth.containsKey(k)) {
                groundTruth.put(k, new StemMultiColMap<HashSet<String>>());
            }
            groundTruth.get(k).put(v, null);

            if (!keysToTuples.containsKey(k)) {
                keysToTuples.put(k, new ArrayList<Tuple>());
            }
            keysToTuples.get(k).add(tuple);
        }

        IndirectMatchingTest.groundTruth = groundTruth;

        WTTransformerIndirect.COVERAGE_THRESHOLD = 2;
        WTTransformerIndirect.verbose = false;

        int[] exampleCounts = new int[]{3};
        // TODO Integer.MAX_VALUE instead of 2 Asli
        testInputEffectSets(exampleCounts, groundTruth, keysToTuples, colsFrom,
                colsTo, fullTable, new AbstractColumnMatcher[0],
                new AbstractColumnMatcher[0], false, true, false,
                Integer.MAX_VALUE, false, ConsolidationMethod.FUNCTIONAL);

    }

    @Test
    public void testIndirectMatchingSetsSyntactic(String file, int[] colsFrom, int[] colsTo, int numberOfExamplePairs) throws Exception {

        inputCSVName = file;

        StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth = new StemMultiColMap<StemMultiColMap<HashSet<String>>>();
        HashMap<List<String>, List<Tuple>> keysToTuples = new HashMap<List<String>, List<Tuple>>();

        Table fullTable = new Table(file);


        for (Tuple tuple : fullTable.getTuples()) {
            List<String> k = tuple.getValuesOfCells(colsFrom);
            List<String> v = tuple.getValuesOfCells(colsTo);

            if (!groundTruth.containsKey(k)) {
                groundTruth.put(k, new StemMultiColMap<HashSet<String>>());
            }
            groundTruth.get(k).put(v, null);

            if (!keysToTuples.containsKey(k)) {
                keysToTuples.put(k, new ArrayList<Tuple>());
            }
            keysToTuples.get(k).add(tuple);
        }

        IndirectMatchingTest.groundTruth = groundTruth;

        WTTransformerIndirect.COVERAGE_THRESHOLD = 2;
        WTTransformerIndirect.verbose = false;

        int[] exampleCounts = new int[]{numberOfExamplePairs};
        // TODO Integer.MAX_VALUE instead of 2 Asli
        testInputEffectSetsSyntactic(exampleCounts, groundTruth, keysToTuples, colsFrom,
                colsTo, fullTable, new AbstractColumnMatcher[0],
                new AbstractColumnMatcher[0], false, true, false,
                Integer.MAX_VALUE, false, ConsolidationMethod.FUNCTIONAL);

    }

    public void testInputEffectTransformations(int exampleCounts,
                                               StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth,
                                               HashMap<List<String>, List<Tuple>> keysToTuples, int[] colsFrom,
                                               int[] colsTo, Table fullTable,
                                               AbstractColumnMatcher[] matchersFrom,
                                               AbstractColumnMatcher[] matchersTo, boolean closedWorld,
                                               boolean weightedExamples, boolean localCompleteness, int iters,
                                               boolean useOutsiderKeys, ConsolidationMethod consolidationMethod, String inputCSVName)
            throws SQLException, IOException, ParseException,
            InterruptedException, ExecutionException, ClassNotFoundException {
        initialExampleCounts = exampleCounts;

        List<List<String>> keys = new ArrayList<List<String>>(groundTruth
                .keySet().size());
        keys.addAll(keysToTuples.keySet());
        // Collections.shuffle(keys);

        // For openWorld
        List<Tuple> tuples = new ArrayList<>();
        for (List<Tuple> tupleList : keysToTuples.values()) {
            tuples.addAll(tupleList);
        }
        // Collections.shuffle(tuples);

        Table inputTable = createInputTable(fullTable, closedWorld, keys,
                keysToTuples, exampleCounts, colsTo, tuples);

        String[] h1 = inputTable.getColumnMapping().getColumnNames(colsFrom);
        String[] h2 = inputTable.getColumnMapping().getColumnNames(colsTo);

        for (int j = 0; j < h1.length; j++) {
            if (h1[j] == null || h1[j].equalsIgnoreCase("COLUMN" + colsFrom[j])) {
                h1[j] = null;
            }
        }

        for (int j = 0; j < h2.length; j++) {
            if (h2[j] == null || h2[j].equalsIgnoreCase("COLUMN" + colsTo[j])) {
                h2[j] = null;
            }
        }

        // finish creating a table as input query
        Class.forName("com.vertica.jdbc.Driver");
        VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection(
                verticaProperties.getProperty("database_host") + verticaProperties.getProperty("database"), verticaProperties);

        VerticaMultiColBatchTableLoader tableLoader = new VerticaMultiColBatchTableLoader(con);
        MultiColTableQuerier queryBuilder = new MultiColVerticaQuerier(con,
                query.multiColumn.MultiColVerticaQuerier.Policy.CERTAIN_FILL_WITH_MOST_CERTAIN,
                50, 2);

        WTTransformerIndirect transformer = new WTTransformerIndirect(
                inputTable, consolidationMethod, colsFrom, colsTo,
                queryBuilder, tableLoader, closedWorld, weightedExamples,
                localCompleteness, iters, null,
                useOutsiderKeys, false, inputCSVName);
        // TODO edit functional test.
        transformer.transformFunctional();
        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> emptyResults = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();
        Pair<Double, Double> pr = precisionRecall(transformer, groundTruth, emptyResults, exampleCounts);
        System.out.println(", Precision: " + pr.key + ", Recall: " + pr.value);
        con.close();

    }

    public void testInputEffectTransformationsSyntactic(int exampleCounts,
                                               StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth,
                                               HashMap<List<String>, List<Tuple>> keysToTuples, int[] colsFrom,
                                               int[] colsTo, Table fullTable,
                                               AbstractColumnMatcher[] matchersFrom,
                                               AbstractColumnMatcher[] matchersTo, boolean closedWorld,
                                               boolean weightedExamples, boolean localCompleteness, int iters,
                                               boolean useOutsiderKeys, ConsolidationMethod consolidationMethod, String inputCSVName)
            throws Exception {
        initialExampleCounts = exampleCounts;

        List<List<String>> keys = new ArrayList<List<String>>(groundTruth
                .keySet().size());
        keys.addAll(keysToTuples.keySet());
        // Collections.shuffle(keys);

        // For openWorld
        List<Tuple> tuples = new ArrayList<>();
        for (List<Tuple> tupleList : keysToTuples.values()) {
            tuples.addAll(tupleList);
        }
        // Collections.shuffle(tuples);

        Table inputTable = createInputTable(fullTable, closedWorld, keys,
                keysToTuples, exampleCounts, colsTo, tuples);

        String[] h1 = inputTable.getColumnMapping().getColumnNames(colsFrom);
        String[] h2 = inputTable.getColumnMapping().getColumnNames(colsTo);

        for (int j = 0; j < h1.length; j++) {
            if (h1[j] == null || h1[j].equalsIgnoreCase("COLUMN" + colsFrom[j])) {
                h1[j] = null;
            }
        }

        for (int j = 0; j < h2.length; j++) {
            if (h2[j] == null || h2[j].equalsIgnoreCase("COLUMN" + colsTo[j])) {
                h2[j] = null;
            }
        }

        // finish creating a table as input query
        Class.forName("com.vertica.jdbc.Driver");
        VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection(
                verticaProperties.getProperty("database_host") + verticaProperties.getProperty("database"), verticaProperties);

        VerticaMultiColBatchTableLoader tableLoader = new VerticaMultiColBatchTableLoader(con);
        MultiColTableQuerier queryBuilder = new MultiColVerticaQuerier(con,
                query.multiColumn.MultiColVerticaQuerier.Policy.CERTAIN_FILL_WITH_MOST_CERTAIN,
                50, 2);

        WTTransformerIndirect transformer = new WTTransformerIndirect(
                inputTable, consolidationMethod, colsFrom, colsTo,
                queryBuilder, tableLoader, closedWorld, weightedExamples,
                localCompleteness, iters, null,
                useOutsiderKeys, false, inputCSVName);
        // TODO edit functional test.
        transformer.transformFunctionalSyntactic();
        con.close();
    //    FileUtils.deleteDirectory(new File("./experiment"));

    }


    public void testInputEffectSets(int[] exampleCounts,
                                    StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth,
                                    HashMap<List<String>, List<Tuple>> keysToTuples, int[] colsFrom,
                                    int[] colsTo, Table fullTable,
                                    AbstractColumnMatcher[] matchersFrom,
                                    AbstractColumnMatcher[] matchersTo, boolean closedWorld,
                                    boolean weightedExamples, boolean localCompleteness, int iters,
                                    boolean useOutsiderKeys, ConsolidationMethod consolidationMethod)
            throws Exception {

        // For closedWorld
        List<List<String>> keys = new ArrayList<List<String>>(groundTruth
                .keySet().size());
        keys.addAll(keysToTuples.keySet());
        // Collections.shuffle(keys);

        // For openWorld
        List<Tuple> tuples = new ArrayList<>();
        for (List<Tuple> tupleList : keysToTuples.values()) {
            tuples.addAll(tupleList);
        }
        // Collections.shuffle(tuples);

        for (int i = 0; i < exampleCounts.length; i++)// number of initial
        // examples
        {
            System.out.println("#initial_examples: " + exampleCounts[i]);
            if (consolidationMethod.equals(ConsolidationMethod.FUNCTIONAL)) {

                testInputEffectTransformations(exampleCounts[i], groundTruth,
                        keysToTuples, colsFrom, colsTo, fullTable,
                        matchersFrom, matchersTo, closedWorld,
                        weightedExamples, localCompleteness, iters,
                        useOutsiderKeys, consolidationMethod, inputCSVName);
            }
        }
    }

    public void testInputEffectSetsSyntactic(int[] exampleCounts,
                                    StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth,
                                    HashMap<List<String>, List<Tuple>> keysToTuples, int[] colsFrom,
                                    int[] colsTo, Table fullTable,
                                    AbstractColumnMatcher[] matchersFrom,
                                    AbstractColumnMatcher[] matchersTo, boolean closedWorld,
                                    boolean weightedExamples, boolean localCompleteness, int iters,
                                    boolean useOutsiderKeys, ConsolidationMethod consolidationMethod)
            throws Exception {

        // For closedWorld
        List<List<String>> keys = new ArrayList<List<String>>(groundTruth
                .keySet().size());
        keys.addAll(keysToTuples.keySet());
        // Collections.shuffle(keys);

        // For openWorld
        List<Tuple> tuples = new ArrayList<>();
        for (List<Tuple> tupleList : keysToTuples.values()) {
            tuples.addAll(tupleList);
        }
        // Collections.shuffle(tuples);

        for (int i = 0; i < exampleCounts.length; i++)// number of initial
        // examples
        {
            System.out.println("#initial_examples: " + exampleCounts[i]);
            if (consolidationMethod.equals(ConsolidationMethod.FUNCTIONAL)) {

                testInputEffectTransformationsSyntactic(exampleCounts[i], groundTruth,
                        keysToTuples, colsFrom, colsTo, fullTable,
                        matchersFrom, matchersTo, closedWorld,
                        weightedExamples, localCompleteness, iters,
                        useOutsiderKeys, consolidationMethod, inputCSVName);
            }
        }
    }


    private Table createInputTable(Table fullTable, boolean closedWorld,
                                   List<List<String>> keys,
                                   HashMap<List<String>, List<Tuple>> keysToTuples, int exampleCount,
                                   int[] colsTo, List<Tuple> tuples) {
        Table inputTable = new Table();
        inputTable.setSchema(fullTable.getColumnMapping());

        if (closedWorld) {

            for (int j = 0; j < exampleCount; j++) {
                List<String> key = keys.get(j);

                // add all its tuples
                for (Tuple tuple : keysToTuples.get(key)) {
                    inputTable.addTuple(new Tuple(tuple));
                }
            }

            for (int j = exampleCount; j < keys.size(); j++) {
                List<String> key = keys.get(j);
                // add all its tuples
                for (Tuple tuple : keysToTuples.get(key)) {
                    Tuple t = new Tuple(tuple);
                    for (int k = 0; k < colsTo.length; k++) {
                        t.getCell(colsTo[k]).setValue(null);
                    }
                    inputTable.addTuple(t);
                }
            }

        } else {

            for (int j = 0; j < exampleCount; j++) {
                inputTable.addTuple(tuples.get(j));
            }
            for (int j = exampleCount; j < tuples.size(); j++) {
                Tuple t = new Tuple(tuples.get(j));
                for (int k = 0; k < colsTo.length; k++) {
                    t.getCell(colsTo[k]).setValue(null);
                }
                inputTable.addTuple(t);

            }
        }
        return inputTable;
    }

    public void printResult(StemMap<StemHistogram> keyToImages) {
        for (String k : keyToImages.keySet()) {
            System.out.println(k);

            StemHistogram imageVals = keyToImages.get(k);
            for (Pair<String, Double> p : imageVals.getCountsSorted()) {
                System.out.println("--" + p.key);
                System.out.println("\t" + p.value);
            }
            System.out
                    .println("================================================");
        }
    }

    public static Pair<Double, Double> precisionRecall(
            WTTransformerIndirect transformer,
            StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth,
            StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> emptyResults, int exampleCounts) {
        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> result = transformer
                .getCurrentAnswer();

        double correctValsFound = 0;
        double wrongValsFound = 0;
        double totalCorrectVals = 0;

        int missedX = 0;

        for (List<String> k : result.keySet()) {

            if (groundTruth.containsKey(k)) {
                if (result.get(k).isEmpty()) {
                    ++missedX;
                    emptyResults.put(k,
                            new StemMultiColMap<HashSet<MultiColMatchCase>>());
                }
                for (List<String> val : result.get(k).keySet()) {
                    if (groundTruth.get(k).containsKey(val)) {
                        correctValsFound++;
                        // correctValsFound += scores.get(k).getScoreOf(val);
                    } else {
                        wrongValsFound++;
                    }
                }
            }
        }
        System.out.println(missedX + " not found");
        for (List<String> k : groundTruth.keySet()) {
            totalCorrectVals += groundTruth.get(k).size();
        }
        correctValsFound = correctValsFound - exampleCounts;
        double precision = correctValsFound * 1.0
                / (correctValsFound + wrongValsFound);
        if (correctValsFound + wrongValsFound == 0) {
            precision = 0;
        }
        double recall = correctValsFound * 1.0 / totalCorrectVals;

        return new Pair<Double, Double>(precision, recall);
    }

    /**
     * DIRTY: just for convenience
     *
     * @param result        used for provenance only to eliminate those in the query
     * @param knownExamples has a single value to be checked
     * @param groundTruth
     * @return
     */
    public static Pair<Double, Double> precisionRecall(
            StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> result,
            StemMultiColMap<StemMultiColHistogram> knownExamples,
            StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth) {

        int correctValsFound = 0;
        int wrongValsFound = 0;
        int totalCorrectVals = 0;
        for (List<String> k : result.keySet()) {
            if (groundTruth.containsKey(k) && knownExamples.containsKey(k)) {
                for (List<String> val : result.get(k).keySet()) {
                    if (groundTruth.get(k).containsKey(val)) {
                        correctValsFound++;
                        // correctValsFound += scores.get(k).getScoreOf(val);
                    } else {
                        wrongValsFound++;

                    }
                }
            }

        }

        for (List<String> k : groundTruth.keySet()) {
            totalCorrectVals += groundTruth.get(k).size();
        }

        double precision = correctValsFound * 1.0
                / (correctValsFound + wrongValsFound);
        if (correctValsFound + wrongValsFound == 0) {
            precision = 0;
        }
        double recall = correctValsFound * 1.0 / totalCorrectVals;

        return new Pair<>(precision, recall);
    }


    public void testTane() throws Exception {
        File dir = new File("/home/jmorcos/webtables/sample");

        for (File fname : dir.listFiles(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith("test.csv");
            }
        })) {
            Table t = new Table(fname.getCanonicalPath());

            System.out.println(t);

            TANEjava tane = new TANEjava(t, 0.1);

            for (FunctionalDependency fd : tane.getFD()) {
                System.out.println(fd);
            }

        }

    }
}
