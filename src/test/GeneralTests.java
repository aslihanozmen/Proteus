package test;

import com.jcraft.jsch.JSchException;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import main.WTTransformerMultiColSets;
import main.WTTransformerMultiColSets.ConsolidationMethod;
import main.WTTransformerMultiColSets.ValidationExamplesSelectionPolicy;
import model.Table;
import model.Tuple;
import model.columnMapping.AbstractColumnMatcher;
import model.multiColumn.MultiColTableLoader;
import model.multiColumn.VerticaMultiColBatchTableLoader;
import model.multiColumn.VerticaMultiColBatchUniontableLoader;

import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.magicwerk.brownies.collections.GapList;
import query.multiColumn.MultiColTableQuerier;
import query.multiColumn.MultiColVerticaQuerier;
import util.*;

import com.vertica.jdbc.VerticaConnection;

public class GeneralTests {

    private Properties verticaProperties = null;

    public String inputCSVName = null;

    public static StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth = null;
    public static  String exampleCountDir = null;
    public static  int initialExampleCounts = 0;
    private static long startingTime;

    // Bruski
    private boolean useUnionTables = false;

    public void setUseUnionTables(boolean useUnionTables) {
        this.useUnionTables = useUnionTables;
    }

    public static long getStartingTime() {
        return startingTime;
    }

    public void setStartingTime(long startingTime) {
        this.startingTime = startingTime;
    }

    @Before
    public void init() throws JSchException {
        startingTime  = System.currentTimeMillis();
        WTTransformerMultiColSets.originalOnly = true;
        verticaProperties = null;
        if (verticaProperties == null) {
            verticaProperties = new Properties();
            verticaProperties.setProperty("user", "username");
            verticaProperties.setProperty("password", "password");
        }
    }

    /**
     * @param file     - File with test data. Column based with groundtruth to asses transformation quality.
     * @param colsFrom - Columns for which we need to find transformations.
     * @param colsTo   - Columns to transform to.
     * @throws Exception
     */
    @Test
    public void testMultiColSets(String file, int[] colsFrom, int[] colsTo, int seed) throws Exception {

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

        GeneralTests.groundTruth = groundTruth;

        //FIXME -  change tau for experiments
        WTTransformerMultiColSets.COVERAGE_THRESHOLD = 2;
        //WTTransformerMultiColSets.COVERAGE_THRESHOLD = 3;
        //WTTransformerMultiColSets.COVERAGE_THRESHOLD = 4;
        //WTTransformerMultiColSets.COVERAGE_THRESHOLD = 5;
        WTTransformerMultiColSets.verbose = false;

        //FIXME - specify number of examples
        int[] exampleCounts = new int[]{3};
        //int[] exampleCounts = new int[]{5};

        //
//        testInputEffectSets(exampleCounts, groundTruth, keysToTuples, colsFrom, colsTo, fullTable,
//                new AbstractColumnMatcher[0], new AbstractColumnMatcher[0], false, true, false,
//                1, false, ConsolidationMethod.FUNCTIONAL, seed);

        testInputEffectSets(exampleCounts, groundTruth, keysToTuples, colsFrom, colsTo, fullTable,
                new AbstractColumnMatcher[0], new AbstractColumnMatcher[0], false, true, false,
                Integer.MAX_VALUE, false, ConsolidationMethod.FUNCTIONAL, seed);

    }

    /**
     * @param file     - File with test data. Column based with groundtruth to asses transformation quality.
     * @param colsFrom - Columns for which we need to find transformations.
     * @param colsTo   - Columns to transform to.
     * @throws Exception
     */
    @Test
    public void testMultiColSetsSyntactical(String file, int[] colsFrom, int[] colsTo, int seed, int numberOfExamplePairs) throws Exception {

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

        GeneralTests.groundTruth = groundTruth;

        //FIXME -  change tau for experiments
        WTTransformerMultiColSets.COVERAGE_THRESHOLD = 2;
        WTTransformerMultiColSets.verbose = false;

        //FIXME - specify number of examples
        int[] exampleCounts = new int[]{numberOfExamplePairs};

        testInputEffectSetsSyntactical(exampleCounts, groundTruth, keysToTuples, colsFrom, colsTo, fullTable,
                new AbstractColumnMatcher[0], new AbstractColumnMatcher[0], false, true, false,
                Integer.MAX_VALUE, false, ConsolidationMethod.FUNCTIONAL, seed);

    }

    public void testNonFunctionalMultiColSets(String file, int[] colsFrom, int[] colsTo, int seed)
            throws Exception {

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

        GeneralTests.groundTruth = groundTruth;

        WTTransformerMultiColSets.COVERAGE_THRESHOLD = 2;
        WTTransformerMultiColSets.verbose = false;

        int[] exampleCounts = new int[]{2};

        testInputEffectSets(exampleCounts, groundTruth, keysToTuples, colsFrom, colsTo, fullTable,
                new AbstractColumnMatcher[0], new AbstractColumnMatcher[0], false, true, false,
                Integer.MAX_VALUE, false, ConsolidationMethod.NON_FUNCTIONAL, seed);

    }

    /**
     * @param exampleCounts
     * @param groundTruth
     * @param keysToTuples
     * @param colsFrom
     * @param colsTo
     * @param fullTable
     * @param matchersFrom
     * @param matchersTo
     * @param closedWorld
     * @param weightedExamples
     * @param localCompleteness
     * @param iters
     * @param useOutsiderKeys
     * @param consolidationMethod
     * @throws SQLException
     * @throws IOException
     * @throws ParseException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void testInputEffectTransformations(int exampleCounts,
                                               StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth,
                                               HashMap<List<String>, List<Tuple>> keysToTuples, int[] colsFrom, int[] colsTo,
                                               Table fullTable, AbstractColumnMatcher[] matchersFrom,
                                               AbstractColumnMatcher[] matchersTo, boolean closedWorld, boolean weightedExamples,
                                               boolean localCompleteness, int iters, boolean useOutsiderKeys,
                                               ConsolidationMethod consolidationMethod, int seed)
            throws Exception {

       // boolean isSemanticTransformation = true;
       initialExampleCounts = exampleCounts;
        List<List<String>> keys = new ArrayList<List<String>>(groundTruth.keySet().size());
        keys.addAll(keysToTuples.keySet());
        // Collections.shuffle(keys);

        // For openWorld
        List<Tuple> tuples = new GapList<>();
        for (List<Tuple> tupleList : keysToTuples.values()) {
            tuples.addAll(tupleList);

        }
        // Collections.shuffle(tuples);
        Collections.sort(tuples);

        Table inputTable = createInputTable(fullTable, closedWorld, keys, keysToTuples,
                exampleCounts, colsTo, tuples, seed);

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
        try {
            Class.forName("com.vertica.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
            VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection("jdbc:vertica://localhost:5433/xformer", verticaProperties);
        //VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection("jdbc:vertica://192.168.56.102/xformer", verticaProperties);

        MultiColTableLoader tableLoader = null;
        MultiColTableQuerier queryBuilder = null;
        if (useUnionTables) {
            tableLoader = new VerticaMultiColBatchUniontableLoader(con);
            queryBuilder = new MultiColVerticaQuerier(con,
                    MultiColVerticaQuerier.Policy.CERTAIN_FILL_WITH_MOST_CERTAIN, 50, 2, useUnionTables);
        } else {
            tableLoader = new VerticaMultiColBatchTableLoader(con);
            queryBuilder = new MultiColVerticaQuerier(con,
                    MultiColVerticaQuerier.Policy.CERTAIN_FILL_WITH_MOST_CERTAIN, 50, 2);
        }

        WTTransformerMultiColSets transformer = new WTTransformerMultiColSets(inputTable,
                consolidationMethod, colsFrom, colsTo, queryBuilder, tableLoader, closedWorld,
                weightedExamples, localCompleteness, iters, matchersFrom, matchersTo, null, false,
                useOutsiderKeys, inputCSVName);
        // TODO edit functional test.


        StemMultiColMap<Pair<List<String>, Double>> results = transformer.transformFunctional();
        System.out.println("RESULTS: " + results);
        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> emptyResults = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();
        Pair<Double, Double> pr = precisionRecall(transformer, groundTruth, emptyResults,
                exampleCounts);
        System.out.println("#Precision:" + pr.key + "\n#Recall:" + pr.value);

        //END OF FUNCT. TRANSF.
        con.close();

    }


    /**
     * @param exampleCounts
     * @param groundTruth
     * @param keysToTuples
     * @param colsFrom
     * @param colsTo
     * @param fullTable
     * @param matchersFrom
     * @param matchersTo
     * @param closedWorld
     * @param weightedExamples
     * @param localCompleteness
     * @param iters
     * @param useOutsiderKeys
     * @param consolidationMethod
     * @throws SQLException
     * @throws IOException
     * @throws ParseException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void testInputEffectTransformationsSyntactical(int exampleCounts,
                                               StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth,
                                               HashMap<List<String>, List<Tuple>> keysToTuples, int[] colsFrom, int[] colsTo,
                                               Table fullTable, AbstractColumnMatcher[] matchersFrom,
                                               AbstractColumnMatcher[] matchersTo, boolean closedWorld, boolean weightedExamples,
                                               boolean localCompleteness, int iters, boolean useOutsiderKeys,
                                               ConsolidationMethod consolidationMethod, int seed)
            throws Exception {

        // boolean isSemanticTransformation = true;
        initialExampleCounts = exampleCounts;
        List<List<String>> keys = new ArrayList<List<String>>(groundTruth.keySet().size());
        keys.addAll(keysToTuples.keySet());
        // Collections.shuffle(keys);

        // For openWorld
        List<Tuple> tuples = new GapList<>();
        for (List<Tuple> tupleList : keysToTuples.values()) {
            tuples.addAll(tupleList);

        }
        // Collections.shuffle(tuples);
        Collections.sort(tuples);

        Table inputTable = createInputTable(fullTable, closedWorld, keys, keysToTuples,
                exampleCounts, colsTo, tuples, seed);

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
        try {
            Class.forName("com.vertica.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection("jdbc:vertica://localhost:5433/xformer", verticaProperties);
        //VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection("jdbc:vertica://192.168.56.102/xformer", verticaProperties);

        MultiColTableLoader tableLoader = null;
        MultiColTableQuerier queryBuilder = null;
        if (useUnionTables) {
            tableLoader = new VerticaMultiColBatchUniontableLoader(con);
            queryBuilder = new MultiColVerticaQuerier(con,
                    MultiColVerticaQuerier.Policy.CERTAIN_FILL_WITH_MOST_CERTAIN, 50, 2, useUnionTables);
        } else {
            tableLoader = new VerticaMultiColBatchTableLoader(con);
            queryBuilder = new MultiColVerticaQuerier(con,
                    MultiColVerticaQuerier.Policy.CERTAIN_FILL_WITH_MOST_CERTAIN, 50, 2);
        }

        WTTransformerMultiColSets transformer = new WTTransformerMultiColSets(inputTable,
                consolidationMethod, colsFrom, colsTo, queryBuilder, tableLoader, closedWorld,
                weightedExamples, localCompleteness, iters, matchersFrom, matchersTo, null, false,
                useOutsiderKeys, inputCSVName);
        // TODO edit functional test.


        transformer.transformFunctionalSyntactical();


        //END OF FUNCT. TRANSF.
        con.close();
    }




    /**
     * @param exampleCounts       - Array with number of examples to choose (3, 5, 10)
     * @param groundTruth         - Truth to evaluate against
     * @param keysToTuples        - Tuple to transform from
     * @param colsFrom            - Array with column indexes to transform from
     * @param colsTo              - Array with column indexes to transform to
     * @param fullTable           - Table created from test file
     * @param matchersFrom        -
     * @param matchersTo
     * @param closedWorld
     * @param weightedExamples
     * @param localCompleteness
     * @param iters               - number of iterations
     * @param useOutsiderKeys
     * @param consolidationMethod - Consolidation method -> [NON_FUNCTIONAL, FUNCTIONAL], MAJORITY not supported
     * @throws Exception
     */
    public void testInputEffectSets(int[] exampleCounts,
                                    StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth,
                                    HashMap<List<String>, List<Tuple>> keysToTuples, int[] colsFrom, int[] colsTo,
                                    Table fullTable, AbstractColumnMatcher[] matchersFrom,
                                    AbstractColumnMatcher[] matchersTo, boolean closedWorld, boolean weightedExamples,
                                    boolean localCompleteness, int iters, boolean useOutsiderKeys,
                                    ConsolidationMethod consolidationMethod, int seed) throws Exception {

        // For closedWorld
        List<List<String>> keys = new ArrayList<List<String>>(groundTruth.keySet().size());
        keys.addAll(keysToTuples.keySet());

        // For openWorld
        List<Tuple> tuples = new ArrayList<>();
        for (List<Tuple> tupleList : keysToTuples.values()) {
            tuples.addAll(tupleList);
        }

        // number of initial examples
        for (int i = 0; i < exampleCounts.length; i++) {

            System.out.println("#initial_examples: " + exampleCounts[i]);
            if (consolidationMethod.equals(ConsolidationMethod.FUNCTIONAL)) {

                testInputEffectTransformations(exampleCounts[i], groundTruth, keysToTuples,
                        colsFrom, colsTo, fullTable, matchersFrom, matchersTo, closedWorld,
                        weightedExamples, localCompleteness, iters, useOutsiderKeys,
                        consolidationMethod, seed);
            }//ENDIF FUNCTIONAL

            if (consolidationMethod.equals(ConsolidationMethod.NON_FUNCTIONAL)) {
                Table inputTable = createInputTable(fullTable, closedWorld, keys, keysToTuples,
                        exampleCounts[i], colsTo, tuples, seed);
                for (ValidationExamplesSelectionPolicy policy : ValidationExamplesSelectionPolicy
                        .values()) {
                    if (policy == ValidationExamplesSelectionPolicy.X_MAX_Y
                            || policy == ValidationExamplesSelectionPolicy.X_MAX_NUM_TABLES
                            || policy == ValidationExamplesSelectionPolicy.ORACLE_BAD_EXAMPLES) {
                        continue;
                    }
                    System.out.println("Example choice policy:" + policy);
                    for (int maxExamplesToValidatePerIter = 10; maxExamplesToValidatePerIter <= 10; maxExamplesToValidatePerIter += 10) {
                        // varies the number of feedback examples to choose per feedback iter
                        for (int supervisedIters = 5; supervisedIters < 6; supervisedIters++) {
                            // changes the number of feedback iters

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
                            VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection("jdbc:vertica://localhost/xformer", verticaProperties);
                            //VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection("jdbc:vertica://192.168.56.102/xformer", "dbadmin", "Transformer");

                            VerticaMultiColBatchTableLoader tableLoader = new VerticaMultiColBatchTableLoader(
                                    con);
                            MultiColTableQuerier queryBuilder = new MultiColVerticaQuerier(
                                    con,
                                    query.multiColumn.MultiColVerticaQuerier.Policy.CERTAIN_FILL_WITH_MOST_CERTAIN,
                                    50, 2);

                            WTTransformerMultiColSets transformer = new WTTransformerMultiColSets(
                                    inputTable, consolidationMethod, colsFrom, colsTo,
                                    queryBuilder, tableLoader, closedWorld, weightedExamples,
                                    localCompleteness, iters, matchersFrom, matchersTo, null,
                                    useOutsiderKeys, false, inputCSVName);
                            boolean stoppedEarly = findNonFunctionalMappings(transformer,
                                    supervisedIters, maxExamplesToValidatePerIter,
                                    exampleCounts[i], iters, policy);
                            if (stoppedEarly) {
                                break;
                            }
                            con.close();
                        }
                    }
                }//ENDFOR VALIDATION POLICY
            }// ENDIF NON_FUNCTIONAL
        }// ENDFOR exampleCount.length
    }

    public void testInputEffectSetsSyntactical(int[] exampleCounts,
                                    StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth,
                                    HashMap<List<String>, List<Tuple>> keysToTuples, int[] colsFrom, int[] colsTo,
                                    Table fullTable, AbstractColumnMatcher[] matchersFrom,
                                    AbstractColumnMatcher[] matchersTo, boolean closedWorld, boolean weightedExamples,
                                    boolean localCompleteness, int iters, boolean useOutsiderKeys,
                                    ConsolidationMethod consolidationMethod, int seed) throws Exception {

        // For closedWorld
        List<List<String>> keys = new ArrayList<List<String>>(groundTruth.keySet().size());
        keys.addAll(keysToTuples.keySet());

        // For openWorld
        List<Tuple> tuples = new ArrayList<>();
        for (List<Tuple> tupleList : keysToTuples.values()) {
            tuples.addAll(tupleList);
        }

        // number of initial examples
        for (int i = 0; i < exampleCounts.length; i++) {

            System.out.println("#initial_examples: " + exampleCounts[i]);
            if (consolidationMethod.equals(ConsolidationMethod.FUNCTIONAL)) {

                testInputEffectTransformationsSyntactical(exampleCounts[i], groundTruth, keysToTuples,
                        colsFrom, colsTo, fullTable, matchersFrom, matchersTo, closedWorld,
                        weightedExamples, localCompleteness, iters, useOutsiderKeys,
                        consolidationMethod, seed);
            }//ENDIF FUNCTIONAL
        }// ENDFOR exampleCount.length
    }

    private boolean findNonFunctionalMappings(WTTransformerMultiColSets transformer,
                                              int supervisedIters, int maxExamplesToValidatePerIter, int exampleCount, int iters,
                                              ValidationExamplesSelectionPolicy policy)
            throws Exception {

        transformer.fuzzyMatching = false;

        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> result = null;
        Pair<Double, Double> pr;

        transformer.toConvergence = false;
        int j = 0;
        int foundExamples = 0;

        for (j = 0; j < supervisedIters; j++) {
            int goodExamples = 0;
            int badExamples = 0;

            transformer.setInductiveIters(1);
            if (policy != ValidationExamplesSelectionPolicy.TruthDiscovery) {
                transformer.setInductiveIters(1);
                transformer.iterate();
            } else {
                transformer.iterateBayesian();
            }
            if (transformer.noMoreTablesLoaded()) {
                break;
            }
            StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> emptyResults = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();
            pr = precisionRecall(transformer, groundTruth, emptyResults, exampleCount);
            System.out.println("Before feedback & pruning: Precision: " + pr.key + ", Recall: "
                    + pr.value);

            if (policy != ValidationExamplesSelectionPolicy.TruthDiscovery) {
                // TODO: repeat?
                List<Pair<List<String>, List<String>>> examples = transformer
                        .selectExamplesForValidation(maxExamplesToValidatePerIter, policy);
                if (examples.isEmpty()) {
                    System.out.println("No more examples to validate...");
                    break;
                }
                if (policy != ValidationExamplesSelectionPolicy.CLUSTERING) {
                    for (Pair<List<String>, List<String>> example : examples) {
                        if (groundTruth.get(example.key).containsKey(example.value)) {
                            transformer.setGoodExample(example.key, example.value);
                            goodExamples++;
                        } else {
                            transformer.setBadExample(example.key, example.value);
                            badExamples++;
                        }
                    }
                } else {
                    for (Pair<List<String>, List<String>> example : examples) {
                        transformer.setBadExample(example.key, example.value);
                        badExamples++;
                    }
                }
                transformer.pruneBadTablesAndAnswers();
                transformer.removeAnswersWithOnlyBadAnswers();
                transformer.runEM();
            }

            emptyResults = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();
            pr = precisionRecall(transformer, groundTruth, emptyResults, exampleCount);
            System.out.println("Supervised iter finished. Good examples: " + goodExamples
                    + ", Bad examples: " + badExamples + ", Precision: " + pr.key + ", Recall: "
                    + pr.value);
        }

        if (policy != ValidationExamplesSelectionPolicy.TruthDiscovery) {
            transformer.setInductiveIters(iters - supervisedIters);
            transformer.toConvergence = false;
            transformer.iterate();

            // added by zia
            transformer.runEM();
            transformer.pruneBadTablesAndAnswers();
        }

        // transformer.removeAnswersWithOnlyBadAnswers();

        result = transformer.getCurrentAnswer();

        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> emptyResults = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();
        pr = precisionRecall(transformer, groundTruth, emptyResults, exampleCount);
        System.out.println(exampleCount + " examples, " + j + " supervised iters used ouf of "
                + supervisedIters + ", " + maxExamplesToValidatePerIter
                + " examples validated per iter, " + pr.key + ", " + pr.value);
        System.out.println();
        System.out.println();

        if (j < supervisedIters) {
            return true;
        }
        return false;

    }

    private Table createInputTable(Table fullTable, boolean closedWorld, List<List<String>> keys,
                                   HashMap<List<String>, List<Tuple>> keysToTuples, int exampleCount, int[] colsTo,
                                   List<Tuple> tuples, int seed) {

        Table inputTable = new Table();
        //For examples
       // try {

            String exampleFilePath = exampleCountDir + "/knownExample.csv";
            System.out.println("Should be same with initial_example : " + exampleCountDir);
            //BufferedWriter writer = Files.newBufferedWriter(Paths.get(exampleFilePath));
            //CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);

            inputTable.setSchema(fullTable.getColumnMapping());

            Random rnd = new Random(seed);
            System.out.println("#seed: " + seed);
            if (closedWorld) {

                TIntSet randomtuples = new TIntHashSet();
                while (randomtuples.size() < exampleCount) {
                    int nextTuple = rnd.nextInt(keys.size());
                    if (randomtuples.contains(nextTuple)) {
                        continue;
                    }
                    randomtuples.add(nextTuple);
                    List<String> key = keys.get(nextTuple);

                    // add all its tuples
                    for (Tuple tuple : keysToTuples.get(key)) {
                        inputTable.addTuple(new Tuple(tuple));
                        System.out.println(tuple);
                      //  csvPrinter.printRecord(tuple.getCell(0), tuple.getCell(1));

                    }
                }

                for (int j = 0; j < keys.size(); j++) {
                    if (randomtuples.contains(j)) {
                        continue;
                    }
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
                TIntSet randomtuples = new TIntHashSet();
//                while (randomtuples.size() < exampleCount) {
//                    int nextTuple = rnd.nextInt(keys.size());
//                    if (randomtuples.contains(nextTuple)) {
//                        continue;
//                    }
//                    randomtuples.add(nextTuple);
//                    inputTable.addTuple(tuples.get(nextTuple));
//                    System.out.println(tuples.get(nextTuple));
//                   // csvPrinter.printRecord(tuples.get(nextTuple).getCell(0), tuples.get(nextTuple).getCell(1));
//
//
               for (int i = 0; i < exampleCount; i++) {
                    if (randomtuples.contains(i)) {
                        continue;
                    }
                    randomtuples.add(i);
                    inputTable.addTuple(tuples.get(i));
                    System.out.println(tuples.get(i));
                   // csvPrinter.printRecord(tuples.get(nextTuple).getCell(0), tuples.get(nextTuple).getCell(1));

                }


                for (int j = 0; j < tuples.size(); j++) {
                    if (randomtuples.contains(j)) {
                        continue;
                    }
                    Tuple t = new Tuple(tuples.get(j));
                    for (int k = 0; k < colsTo.length; k++) {
                        t.getCell(colsTo[k]).setValue(null);
                    }
                    inputTable.addTuple(t);//System.out.println(t);

                }
            }
           // csvPrinter.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        return inputTable;

    }

    public void createKnownExampleFile(String inputCSVPath, int exampleCount) throws IOException {
        String inputCSVName = new File(inputCSVPath).getName();
        String dirPath = "/Users/aslihanozmen/Documents/DataTransformationDiscoveryThesis-master/benchmark/syntacticBenchmarkResult/benchmark-stackoverflow/" + inputCSVName;
        File file = new File(dirPath);
        if (!file.exists()) {
            createDirectory(dirPath);
        }

        exampleCountDir = dirPath + "/" + exampleCount;
        File exampleCountDirFile = new File(exampleCountDir);
        if (!exampleCountDirFile.exists()) {
            createDirectory(exampleCountDir);
        }
    }

    public void createDirectory(String dirPath) throws IOException {
        Path path = Paths.get(dirPath);
        if (!Files.exists(Paths.get(dirPath))) {
            Files.createDirectory(path);
            System.out.println("Directory is created!");
        }
    }

    public static Pair<Double, Double>
    precisionRecall(WTTransformerMultiColSets transformer,
                                                       StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth,
                                                       StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> emptyResults,
                                                       double exampleCount) throws IOException {

        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> result = transformer.getCurrentAnswer();

        double correctValsFound = 0;
        double wrongValsFound = 0;
        double totalCorrectVals = 0;
        int examplesFound = 0;

        int missedX = 0;

        // create csv files which has missing values from the previous transformation
        String inputCSVPath = transformer.getInputCSVName();
        String inputCSVName = new File(inputCSVPath).getName();
        File dir = new File("./benchmarkMultiColFunctional/Functional");
        if(!dir.exists()) {
            if(dir.mkdirs()) {
                System.out.println("Directory for benchmark is created");
            }
        }


        File inputCsvFile = new File(dir, inputCSVName);
           if(inputCsvFile.createNewFile()){
              System.out.println("Recreating the file");
           } else {
               PrintWriter writer = new PrintWriter(inputCsvFile);
               writer.print("");
               writer.close();
           }

        FileWriter csvWriter = new FileWriter(inputCsvFile.getAbsolutePath());

        for (List<String> strings : transformer.getInitialExamplesFound().keySet()) {
            if (result.containsKey(strings)) {
                examplesFound++;
            }
        }

        for (List<String> k : result.keySet()) {

            if (groundTruth.containsKey(k)) {
                if (result.get(k).isEmpty()) {
                    //FIXME
                    //System.out.println(Arrays.toString(k.toArray()));

                    ++missedX;
                    emptyResults.put(k, new StemMultiColMap<>());
                    writeMissingAndWrongFoundValsToCSV(groundTruth, csvWriter, k);

                }
                for (List<String> val : result.get(k).keySet()) {
                    if (groundTruth.get(k).containsKey(val)) {
                        correctValsFound++;
                    } else {
                        wrongValsFound++;
                        writeMissingAndWrongFoundValsToCSV(groundTruth, csvWriter, k);
                    }
                }

            }
        }

        csvWriter.flush();
        csvWriter.close();
        System.out.println("#not_found:" + missedX);
        for (List<String> k : groundTruth.keySet()) {
            totalCorrectVals += groundTruth.get(k).size();
        }

        correctValsFound = correctValsFound - examplesFound;
        totalCorrectVals = totalCorrectVals - examplesFound;

        double precision = correctValsFound * 1.0 / (correctValsFound + wrongValsFound);
        if (correctValsFound + wrongValsFound == 0) {
            precision = 0;
        }
        double recall = correctValsFound * 1.0 / totalCorrectVals;

        File resultByDataXFormerCsvFile = new File(dir, "results_by_DataXFormer_for_" + inputCSVName);
        if(resultByDataXFormerCsvFile.createNewFile()){
            System.out.println("Recreating the file");
        } else {
            PrintWriter writer = new PrintWriter(resultByDataXFormerCsvFile);
            writer.print("");
            writer.close();
        }
        FileWriter resultCsvWriter = new FileWriter(resultByDataXFormerCsvFile.getAbsolutePath());

        resultCsvWriter.append("not_found").append(",").append(String.valueOf(missedX));
        resultCsvWriter.append("\n");
        resultCsvWriter.append("correctValsFound").append(",").append(String.valueOf(correctValsFound));
        resultCsvWriter.append("\n");
        resultCsvWriter.append("totalCorrectVals").append(",").append(String.valueOf(totalCorrectVals));
        resultCsvWriter.append("\n");
        resultCsvWriter.append("precision").append(",").append(String.valueOf(precision));
        resultCsvWriter.append("\n");
        resultCsvWriter.append("recall").append(",").append(String.valueOf(recall));
        resultCsvWriter.append("\n");
        resultCsvWriter.append("runtime").append(",").append(String.valueOf(System.currentTimeMillis() - getStartingTime()));

        resultCsvWriter.flush();
        resultCsvWriter.close();

        System.out.println("#wrong_values" + wrongValsFound);
        System.out.println("#correctValsFound" + correctValsFound);
        System.out.println("#totalCorrectVals" + totalCorrectVals);
        return new Pair<>(precision, recall);
    }

    private static void writeMissingAndWrongFoundValsToCSV(StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth, FileWriter csvWriter, List<String> k) throws IOException {
        System.out.println("Not Found Values: " + k);

        if (k.size() == 2) {
            csvWriter.append(k.get(0)).append(',').append(k.get(1)).append(",")
                    .append(String.valueOf(groundTruth.get(k).keySet().iterator().next())
                            .replace("[", "").replace("]", ""));
            csvWriter.append("\n");
        } else if (k.size() == 1) {
            csvWriter.append(k.get(0)).append(',')
                    .append(String.valueOf(groundTruth.get(k).keySet().iterator().next())
                            .replace("[", "").replace("]", ""));
            csvWriter.append("\n");
        }
    }

  /*  public static void createDirectoryForExampleCount(String dirPath) throws IOException {
        Path path = Paths.get(dirPath);
        if (!Files.exists(Paths.get(dirPath))) {
            Files.createDirectories(path);
            //System.out.println("Directory is created!");
        }
//        else {
//            System.out.println("Directory is already exist!");
//        }

   // } */

    /**
     * Compare resultset and groundtruth set and calculate precision and recall.
     *
     * @return
     */
//    public static Pair<Double, Double> precisionRecall(StemMap<StemMap<HashSet<MatchCase>>> result,
//                                                       StemMap<StemMap<HashSet<String>>> groundTruth) {
//        int correctValsFound = 0;
//        int wrongValsFound = 0;
//        int totalCorrectVals = 0;
//        for (String k : result.keySet()) {
//            if (groundTruth.containsKey(k)) {
//                for (String val : result.get(k).keySet()) {
//                    if (groundTruth.get(k).containsKey(val)) {
//                        correctValsFound++;
//                    } else {
//                        wrongValsFound++;
//                    }
//                }
//            }
//        }
//
//        for (String k : groundTruth.keySet()) {
//            totalCorrectVals += groundTruth.get(k).size();
//        }
//
//        double precision = correctValsFound * 1.0 / (correctValsFound + wrongValsFound);
//        if (correctValsFound + wrongValsFound == 0) {
//            precision = 0;
//        }
//        double recall = correctValsFound * 1.0 / totalCorrectVals;
//
//        return new Pair<>(precision, recall);
//    }
    public static double similarity(String a, String b) {
        return 1 - editDistance(a, b);
    }

    public static double editDistance(String a, String b) {
        return EditDistance.editDistance(a, b);
    }

    @After
    public void finish() {
    }


    //******************************************************************************************************************
    //******************************************************************************************************************

//    public void testCoocc() throws SQLException {
//        VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection(
//                verticaProperties.getProperty("database_host") + verticaProperties.getProperty("database"), verticaProperties);
////        VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection(
////                databaseHost + verticaDatabase, verticaUser, verticaPassword);
//
//        VerticaQuerier vq = new VerticaQuerier(con);
//
//        ArrayList<String> vals = new ArrayList<String>();
//        vals.add("australia");
//        vals.add("germany");
//        vals.add("bhutan");
//        vals.add("paris");
//
//        Histogram<String> result = vq.getCooccFreq("france", vals);
//        System.out.println(result.getCountsUnsorted());
//        System.out.println(result.getCountsSorted());
//
//        con.close();
//    }

//    public void testGenQuestions() throws Exception {
//        StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth = new StemMultiColMap<StemMultiColMap<HashSet<String>>>();
//        HashMap<List<String>, List<Tuple>> keysToTuples = new HashMap<List<String>, List<Tuple>>();
//
//
//        Table fullTable = new Table("benchmark/functional/prettycleaned_airport_codes_internalGT2.csv");
//        int[] colsFrom = new int[]{0};
//        int[] colsTo = new int[]{1};
//
//        for (Tuple tuple : fullTable.getTuples()) {
//            List<String> k = tuple.getValuesOfCells(colsFrom);
//            List<String> v = tuple.getValuesOfCells(colsTo);
//
//            if (!groundTruth.containsKey(k)) {
//                groundTruth.put(k, new StemMultiColMap<HashSet<String>>());
//            }
//            groundTruth.get(k).put(v, null);
//
//            if (!keysToTuples.containsKey(k)) {
//                keysToTuples.put(k, new ArrayList<Tuple>());
//            }
//            keysToTuples.get(k).add(tuple);
//        }
//
//        WTTransformerMultiColSets.COVERAGE_THRESHOLD = 2;
//        WTTransformerMultiColSets.verbose = false;
//        int[] exampleCounts = new int[]{5};
//
//        // For closedWorld
//        List<List<String>> keys = new ArrayList<List<String>>(groundTruth.keySet().size());
//        keys.addAll(keysToTuples.keySet());
//        // Collections.shuffle(keys);
//
//        // For openWorld
//        List<Tuple> tuples = new ArrayList<>();
//        for (List<Tuple> tupleList : keysToTuples.values()) {
//            tuples.addAll(tupleList);
//        }
//        // Collections.shuffle(tuples);
//        for (int i = 0; i < exampleCounts.length; i++) {
//            HashMap<List<String>, HashSet<List<String>>> exampleKeys = new HashMap<List<String>, HashSet<List<String>>>();
//            ArrayList<List<String>> queryKeys = new ArrayList<List<String>>();
//
//            Table inputTable = new Table();
//            inputTable.setSchema(fullTable.getColumnMapping());
//
//            for (int j = 0; j < exampleCounts[i]; j++) {
//                List<String> key = keys.get(j);
//
//                HashSet<List<String>> answerSet = new HashSet<List<String>>();
//
//                // add all its tuples
//                for (Tuple tuple : keysToTuples.get(key)) {
//                    inputTable.addTuple(new Tuple(tuple));
//                    answerSet.add(tuple.getValuesOfCells(colsTo));
//
//                }
//                exampleKeys.put(key, answerSet);
//            }
//
//            for (int j = exampleCounts[i]; j < keys.size(); j++) {
//                List<String> key = keys.get(j);
//                queryKeys.add(key);
//                // add all its tuples
//                for (Tuple tuple : keysToTuples.get(key)) {
//                    Tuple t = new Tuple(tuple);
//                    for (int k = 0; k < colsTo.length; k++) {
//                        t.getCell(colsTo[k]).setValue(null);
//                    }
//                    inputTable.addTuple(t);
//                }
//            }
//
//            QuestionGenerator qGen = new QuestionGenerator();
//            ArrayList<String> headersFrom = new ArrayList<String>();
//            for (String h : inputTable.getColumnMapping().getColumnNames(colsFrom)) {
//                headersFrom.add(h);
//            }
//            ArrayList<String> headersTo = new ArrayList<String>();
//            for (String h : inputTable.getColumnMapping().getColumnNames(colsTo)) {
//                headersTo.add(h);
//            }
//
//            ArrayList<Question> questions = qGen.generateQuestionsRandomPacking(headersFrom,
//                    headersTo, exampleKeys, queryKeys, 3, 2);
//
//            for (int j = 0; j < questions.size(); j++) {
//                questions.get(j).setQuestionId(j);
//            }
//
//            //FIXME Run Crowdclient and update URL and Port accordingly
//            CrowdClient client = new CrowdClient("http://localhost:8880/DataXFormer_Crowd/");
//            client.authenticate("admin", "admin");
//
//            System.out.println("clear category: " + client.clearQuestionsByCategory("testXFormer"));
//            System.out.println("Posted questions in category: "
//                    + client.postQuestions(questions, "testXFormer", PostMode.INTERNAL));
//
//            ArrayList<Answer> answers = new ArrayList<>();
//            Date lastUpdate = new Date();
//            while (answers.size() < questions.size()) {
//                Date currentTime = new Date();
//                List<Answer> retrievedAnswers = client.queryAnswersByLastUpdateDate(lastUpdate,
//                        currentTime, "testXFormer");
//
//                for (Answer answer : retrievedAnswers) {
//                    System.out.println("Q" + answer.getQuestionID() + ": " + answer.getContent());
//                }
//
//                answers.addAll(retrievedAnswers);
//                lastUpdate = currentTime;
//
//                Thread.sleep(5000);
//            }
//            client.close();
//        }
//    }

//    public void testKB(String inputTableDir) throws Exception {
//
//        Table fullTable = new Table(inputTableDir);
//        int[] colsFrom = new int[]{1};
//        int[] colsTo = new int[]{0};
//
//        StemMap<StemMap<HashSet<String>>> groundTruth = new StemMap<StemMap<HashSet<String>>>();
//        HashMap<String, List<Tuple>> keysToTuples = new HashMap<String, List<Tuple>>();
//
//        for (Tuple tuple : fullTable.getTuples()) {
//            String k = tuple.getValuesOfCells(colsFrom).get(0);
//            String v = tuple.getValuesOfCells(colsTo).get(0);
//
//            if (!groundTruth.containsKey(k)) {
//                groundTruth.put(k, new StemMap<HashSet<String>>());
//            }
//            groundTruth.get(k).put(v, null);
//
//            if (!keysToTuples.containsKey(k)) {
//                keysToTuples.put(k, new ArrayList<Tuple>());
//            }
//            keysToTuples.get(k).add(tuple);
//        }
//
//        // For openWorld
//        List<Tuple> tuples = new ArrayList<>();
//        for (List<Tuple> tupleList : keysToTuples.values()) {
//            tuples.addAll(tupleList);
//        }
//        // Collections.shuffle(tuples);
//
//        tuples = fullTable.getTuples();
//
//        int[] exampleCounts = new int[]{5};
//
//        for (int i = 0; i < exampleCounts.length; i++) {
//            Table inputTable = new Table();
//            inputTable.setSchema(fullTable.getColumnMapping());
//
//            qa.qcri.katara.dbcommon.Table examplesAsATable = new qa.qcri.katara.dbcommon.Table(
//                    inputTableDir, 0);
//            HashSet<qa.qcri.katara.dbcommon.Tuple> tuplesToRemove = new HashSet<>();
//            tuplesToRemove.add(examplesAsATable.getTuples().get(0));
//            examplesAsATable.removeTuple(tuplesToRemove);
//
//            Set<qa.qcri.katara.dbcommon.Tuple> tuplesToInsert = new HashSet<>();
//
//            for (int j = 0; j < exampleCounts[i]; j++) {
//                inputTable.addTuple(tuples.get(j));
//
//                String[] vals = new String[inputTable.getNumCols()];
//                for (int m = 0; m < inputTable.getNumCols(); m++) {
//                    vals[m] = tuples.get(j).getCell(m).getValue();
//                }
//                qa.qcri.katara.dbcommon.Tuple tup = new qa.qcri.katara.dbcommon.Tuple(vals,
//                        examplesAsATable.getColumnMapping(), j);
//                tuplesToInsert.add(tup);
//            }
//            examplesAsATable.insertTuples(tuplesToInsert);
//
//            for (int j = exampleCounts[i]; j < tuples.size(); j++) {
//                Tuple t = new Tuple(tuples.get(j));
//                for (int k = 0; k < colsTo.length; k++) {
//                    t.getCell(colsTo[k]).setValue(null);
//                }
//                inputTable.addTuple(t);
//
//            }
//
//            String[] h1 = inputTable.getColumnMapping().getColumnNames(colsFrom);
//            String[] h2 = inputTable.getColumnMapping().getColumnNames(colsTo);
//
//            for (int j = 0; j < h1.length; j++) {
//                if (h1[j] == null || h1[j].equalsIgnoreCase("COLUMN" + colsFrom[j])) {
//                    h1[j] = null;
//                }
//            }
//
//            for (int j = 0; j < h2.length; j++) {
//                if (h2[j] == null || h2[j].equalsIgnoreCase("COLUMN" + colsTo[j])) {
//                    h2[j] = null;
//                }
//            }
//
//            KnowledgeDatabaseConfig.setSampling(Integer.MAX_VALUE);
//            KnowledgeDatabaseConfig.maxMatches = -1;
//            KnowledgeDatabaseConfig.frequentPercentage = 0.01;
//            KnowledgeDatabaseConfig.KBStatsDirectoryBase = "/Users/abedjan/yagodata";
//            KnowledgeDatabaseConfig.dataDirectoryBase = "/Users/abedjan/simpleyago";
//
//            KBReader kbr = new KBReader("/Users/abedjan/yagodata");
//
//            PatternGenerationRankJoin pgrj = new PatternGenerationRankJoin(examplesAsATable,
//                    // new qa.qcri.katara.dbcommon.Table(inputTableDir,
//                    // Integer.MAX_VALUE),
//                    kbr, 10, "/Users/abedjan/yagodata/out.txt", 1);
//            pgrj.rankJoin(false);
//
//            TableSemantics pattern = pgrj.getTopKTableSemantics().get(0);
//
//            boolean flipped = false;
//            boolean keyColComesFirst = colsFrom[0] < colsTo[0];
//            String bestRelURI;
//
//            if (keyColComesFirst) {
//                bestRelURI = pattern.col2Rel.get(colsFrom[0] + "," + colsTo[0]);
//            } else {
//                bestRelURI = pattern.col2Rel.get(colsTo[0] + "," + colsFrom[0]);
//                ;
//            }
//
//            if (bestRelURI.startsWith(TableSemantics.REL_REVERSED_TAG)) {
//                flipped = true;
//                bestRelURI = bestRelURI.substring(TableSemantics.REL_REVERSED_TAG.length());
//            }
//
//            String type1 = pattern.col2Type.get(Integer.toString(colsFrom[0]));
//
//            StemMap<StemMap<HashSet<MatchCase>>> result = new StemMap<StemMap<HashSet<MatchCase>>>();
//            for (Tuple tuple : inputTable.getTuples()) {
//                String k = tuple.getValuesOfCells(colsFrom).get(0);
//                if (!result.containsKey(k)) {
//                    result.put(k, new StemMap<HashSet<MatchCase>>());
//                }
//                String v = tuple.getValuesOfCells(colsTo).get(0);
//                if (v != null) {
//                    if (!result.get(k).containsKey(v)) {
//                        result.get(k).put(v, null);
//                    }
//                } else {
//                    Set<String> kEntities = kbr.getEntitiesWithLabel(k);
//                    for (String kEntity : kEntities) {
//                        HashSet<String> types = kbr.getTypes(kEntity, true);
//                        if (!types.contains(type1)) {
//                            continue;
//                        }
//
//                        Set<String> vEntities;
//                        if ((keyColComesFirst && !flipped) || (!keyColComesFirst && flipped)) {
//                            vEntities = kbr
//                                    .getObjectEntitiesGivenRelAndSubject(bestRelURI, kEntity);
//                        } else {
//                            vEntities = kbr
//                                    .getSubjectEntitiesGivenRelAndObject(bestRelURI, kEntity);
//                        }
//                        for (String vEntity : vEntities) {
//                            if (vEntity.startsWith("http://")) {
//                                HashSet<String> vs = kbr.getLabels(vEntity);
//                                for (String val : vs) {
//                                    val = vEntity;
//                                    if (!result.get(k).containsKey(val)) {
//                                        result.get(k).put(val, new HashSet<MatchCase>());
//                                    }
//                                }
//                            } else {
//                                result.get(k).put(vEntity, new HashSet<MatchCase>());
//                            }
//                        }
//                    }
//
//                }
//            }
//
//            kbr.close();
//
//            int keysWithResults = 0;
//            for (String k : result.keySet()) {
//                System.out.println();
//                System.out.println();
//
//                System.out.println(k);
//                System.out.println("--------");
//
//                if (result.get(k) != null && result.get(k).size() > 0) {
//                    keysWithResults++;
//                }
//                for (String v : result.get(k).keySet()) {
//                    System.out.println(v);
//                }
//            }
//
//            System.out.println("Recall: " + (keysWithResults * 1.0 / result.size()));
//            Pair<Double, Double> pr = precisionRecall(result, groundTruth);
//            System.out.println("precision: " + pr.key + "\t\trecall:" + pr.value);
//
//        }
//
//    }

//    public void testKBSimple(String inputTableDir) throws Exception {
//        Table fullTable = new Table(inputTableDir);
//        int[] colsFrom = new int[]{0};
//        int[] colsTo = new int[]{1};
//
//        StemMap<StemMap<HashSet<String>>> groundTruth = new StemMap<StemMap<HashSet<String>>>();
//        HashMap<String, List<Tuple>> keysToTuples = new HashMap<String, List<Tuple>>();
//
//        for (Tuple tuple : fullTable.getTuples()) {
//            String k = tuple.getValuesOfCells(colsFrom).get(0);
//            String v = tuple.getValuesOfCells(colsTo).get(0);
//
//            if (!groundTruth.containsKey(k)) {
//                groundTruth.put(k, new StemMap<HashSet<String>>());
//            }
//            groundTruth.get(k).put(v, null);
//
//            if (!keysToTuples.containsKey(k)) {
//                keysToTuples.put(k, new ArrayList<Tuple>());
//            }
//            keysToTuples.get(k).add(tuple);
//        }
//
//        // For openWorld
//        List<Tuple> tuples = new ArrayList<>();
//        for (List<Tuple> tupleList : keysToTuples.values()) {
//            tuples.addAll(tupleList);
//        }
//        // Collections.shuffle(tuples);
//        int[] exampleCounts = new int[]{5};
//
//        for (int i = 0; i < exampleCounts.length; i++) {
//            Table inputTable = new Table();
//            inputTable.setSchema(fullTable.getColumnMapping());
//
//            for (int j = 0; j < exampleCounts[i]; j++) {
//                inputTable.addTuple(tuples.get(j));
//            }
//            for (int j = exampleCounts[i]; j < tuples.size(); j++) {
//                Tuple t = new Tuple(tuples.get(j));
//                for (int k = 0; k < colsTo.length; k++) {
//                    t.getCell(colsTo[k]).setValue(null);
//                }
//                inputTable.addTuple(t);
//
//            }
//
//            String[] h1 = inputTable.getColumnMapping().getColumnNames(colsFrom);
//            String[] h2 = inputTable.getColumnMapping().getColumnNames(colsTo);
//
//            for (int j = 0; j < h1.length; j++) {
//                if (h1[j] == null || h1[j].equalsIgnoreCase("COLUMN" + colsFrom[j])) {
//                    h1[j] = null;
//                }
//            }
//
//            for (int j = 0; j < h2.length; j++) {
//                if (h2[j] == null || h2[j].equalsIgnoreCase("COLUMN" + colsTo[j])) {
//                    h2[j] = null;
//                }
//            }
//
//            KnowledgeDatabaseConfig.setSampling(Integer.MAX_VALUE);
//            KnowledgeDatabaseConfig.maxMatches = -1;
//            KnowledgeDatabaseConfig.frequentPercentage = 0.05;
//            KnowledgeDatabaseConfig.KBStatsDirectoryBase = "/Users/abedjan/yagodata";
//
//            KBReader kbr = new KBReader("/Users/abedjan/yagodata");
//
//            Histogram<String> scoresOfUnflippedRels = new Histogram<String>();
//            Histogram<String> scoresOfFlippedRels = new Histogram<String>();
//
//            for (Tuple tuple : inputTable.getTuples()) {
//                if (tuple.getCell(colsTo[0]).getValue() != null) {
//                    String k = tuple.getValuesOfCells(colsFrom).get(0);
//                    String v = tuple.getValuesOfCells(colsTo).get(0);
//
//                    for (String r : kbr.getDirectRelationShips(k, v, true)) {
//                        scoresOfUnflippedRels.increment(r);
//                    }
//
//                    for (String r : kbr.getDirectRelationShips(v, k, true)) {
//                        scoresOfFlippedRels.increment(r);
//                    }
//
//                }
//
//            }
//
//            List<Pair<String, Double>> sortedUnflipped = scoresOfUnflippedRels.getCountsSorted();
//            List<Pair<String, Double>> sortedFlipped = scoresOfFlippedRels.getCountsSorted();
//
//            boolean flipped = false;
//            String bestRelURI = null;
//            if (sortedUnflipped.size() == 0 && sortedFlipped.size() == 0) {
//                // None found
//                System.err.println("No rels found. Sorry!");
//                System.exit(0);
//            } else if (sortedUnflipped.size() == 0 && sortedFlipped.size() > 0) {
//                flipped = true;
//                bestRelURI = sortedFlipped.get(0).key;
//            } else if (sortedUnflipped.size() > 0 && sortedFlipped.size() == 0) {
//                flipped = false;
//                bestRelURI = sortedUnflipped.get(0).key;
//            } else {
//                if (sortedUnflipped.get(0).value >= sortedFlipped.get(0).value) {
//                    flipped = true;
//                    bestRelURI = sortedFlipped.get(0).key;
//                } else {
//                    flipped = false;
//                    bestRelURI = sortedUnflipped.get(0).key;
//                }
//            }
//
//            StemMap<StemMap<HashSet<MatchCase>>> result = new StemMap<StemMap<HashSet<MatchCase>>>();
//            for (Tuple tuple : inputTable.getTuples()) {
//                String k = tuple.getValuesOfCells(colsFrom).get(0);
//                if (!result.containsKey(k)) {
//                    result.put(k, new StemMap<HashSet<MatchCase>>());
//                }
//                String v = tuple.getValuesOfCells(colsTo).get(0);
//                if (v != null) {
//                    if (!result.get(k).containsKey(v)) {
//                        result.get(k).put(v, null);
//                    }
//                } else {
//                    Set<String> kEntities = kbr.getEntitiesWithLabel(k);
//                    for (String kEntity : kEntities) {
//                        Set<String> vEntities;
//                        if (!flipped) {
//                            vEntities = kbr
//                                    .getObjectEntitiesGivenRelAndSubject(bestRelURI, kEntity);
//                        } else // if flipped
//                        {
//                            vEntities = kbr
//                                    .getSubjectEntitiesGivenRelAndObject(bestRelURI, kEntity);
//                        }
//                        for (String vEntity : vEntities) {
//                            HashSet<String> vs = kbr.getLabels(vEntity);
//                            for (String val : vs) {
//                                val = vEntity;
//                                if (!result.get(k).containsKey(val)) {
//                                    result.get(k).put(val, new HashSet<MatchCase>());
//                                }
//                            }
//                        }
//                    }
//
//                }
//            }
//
//            kbr.close();
//
//            Pair<Double, Double> pr = precisionRecall(result, groundTruth);
//            System.out.println("precision: " + pr.key + "\t\trecall:" + pr.value);
//
//            for (String k : result.keySet()) {
//                System.out.println(k);
//                System.out.println("--------");
//                for (String v : result.get(k).keySet()) {
//                    System.out.println(v);
//                }
//            }
//        }
//
//    }

    /**
     * @param groundTruth
     * @return
     */
//    private HashMap<List<String>, List<Tuple>> createTuplesFromGroundTruth(
//            StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth, Table table) {
//
//        HashMap<List<String>, List<Tuple>> keyToTuples = new HashMap<List<String>, List<Tuple>>();
//
//        for (List<String> k : groundTruth.keySet()) {
//            List<Tuple> tuples = new ArrayList<Tuple>();
//
//            for (List<String> v : groundTruth.get(k).keySet()) {
//                String[] tupAsArr = new String[k.size() + v.size()];
//                for (int i = 0; i < k.size(); i++) {
//                    tupAsArr[i] = k.get(i);
//                }
//                for (int i = 0; i < v.size(); i++) {
//                    tupAsArr[k.size() + i] = v.get(i);
//                }
//                Tuple tuple = new Tuple(tupAsArr, table.getColumnMapping(), -1);
//                tuples.add(tuple);
//            }
//
//            keyToTuples.put(k, tuples);
//        }
//        return keyToTuples;
//    }

    /**
     * @param groundTruth
     * @return
     */
//    private HashMap<String, List<Tuple>> createTuplesFromGroundTruth(
//            StemMap<StemMap<HashSet<String>>> groundTruth, TableSchema schema) {
//
//        HashMap<String, List<Tuple>> keyToTuples = new HashMap<String, List<Tuple>>();
//
//        for (String k : groundTruth.keySet()) {
//            List<Tuple> tuples = new ArrayList<Tuple>();
//
//            for (String v : groundTruth.get(k).keySet()) {
//                Tuple tuple = new Tuple(new String[]{k, v}, schema, -1);
//                tuples.add(tuple);
//            }
//
//            keyToTuples.put(k, tuples);
//        }
//        return keyToTuples;
//    }
//
//    private HashMap<String, List<Tuple>> createTuplesFromGroundTruth(
//            HashMap<String, HashSet<String>> groundTruth, TableSchema schema) {
//
//        HashMap<String, List<Tuple>> keyToTuples = new HashMap<String, List<Tuple>>();
//
//        for (String k : groundTruth.keySet()) {
//            List<Tuple> tuples = new ArrayList<Tuple>();
//
//            for (String v : groundTruth.get(k)) {
//                Tuple tuple = new Tuple(new String[]{k, v}, schema, -1);
//                tuples.add(tuple);
//            }
//
//            keyToTuples.put(k, tuples);
//        }
//        return keyToTuples;
//    }
//
//    public void printResult(StemMap<StemHistogram> keyToImages) {
//        for (String k : keyToImages.keySet()) {
//            System.out.println(k);
//
//            StemHistogram imageVals = keyToImages.get(k);
//            for (Pair<String, Double> p : imageVals.getCountsSorted()) {
//                System.out.println("--" + p.key);
//                System.out.println("\t" + p.value);
//            }
//            System.out.println("================================================");
//        }
//    }

//    public static Pair<Double, Double> precisionRecall(WTTransformerMultiColSets transformer,
//                                                       StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth,
//                                                       VerticaMultiColSimilarityQuerier queryBuilder) throws IOException {
//        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> provenance = transformer
//                .getProvenance();
//        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> result = transformer
//                .getCurrentAnswer();
//
//        StemMultiColMap<StemMultiColHistogram> scores = transformer.getAllAnswerRatings();
//
//        double correctValsFound = 0;
//        double wrongValsFound = 0;
//        double totalCorrectVals = 0;
//
//        for (List<String> k : result.keySet()) {
//            if (groundTruth.containsKey(k)) {
//                for (List<String> val : result.get(k).keySet()) {
//                    if (groundTruth.get(k).containsKey(val)) {
//                        correctValsFound++;
//                        // correctValsFound += scores.get(k).getScoreOf(val);
//                    } else {
//                        boolean noMatch = true;
//
//                        for (List<String> v : groundTruth.get(k).keySet()) {
//                            if (queryBuilder.areSimilar(v, val)) {
//                                noMatch = false;
//                                break;
//                            }
//                        }
//
//                        if (noMatch) {
//                            wrongValsFound++;
//                        } else {
//                            correctValsFound++;
//                        }
//                        // wrongValsFound += scores.get(k).getScoreOf(val);
//                    }
//                }
//            }
//        }
//
//        for (List<String> k : groundTruth.keySet()) {
//            totalCorrectVals += groundTruth.get(k).size();
//        }
//
//        double precision = correctValsFound * 1.0 / (correctValsFound + wrongValsFound);
//        if (correctValsFound + wrongValsFound == 0) {
//            precision = 0;
//        }
//        double recall = correctValsFound * 1.0 / totalCorrectVals;
//
//        return new Pair<Double, Double>(precision, recall);
//    }

    /**
     * DIRTY: just for convenience
     *
     * @param result        used for provenance only to eliminate those in the query
     * @param knownExamples has a single value to be checked
     * @param groundTruth
     * @return
     */
//    public static Pair<Double, Double> precisionRecall(
//            StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> result,
//            StemMultiColMap<StemMultiColHistogram> knownExamples,
//            StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth) {
//
//        int correctValsFound = 0;
//        int wrongValsFound = 0;
//        int totalCorrectVals = 0;
//        for (List<String> k : result.keySet()) {
//            if (groundTruth.containsKey(k) && knownExamples.containsKey(k)) {
//                for (List<String> val : result.get(k).keySet()) {
//
//                    if (groundTruth.get(k).containsKey(val)) {
//                        correctValsFound++;
//                        // correctValsFound += scores.get(k).getScoreOf(val);
//                    } else {
//                        wrongValsFound++;
//
//                    }
//                }
//            }
//
//        }
//
//        for (List<String> k : groundTruth.keySet()) {
//            totalCorrectVals += groundTruth.get(k).size();
//        }
//
//        double precision = correctValsFound * 1.0 / (correctValsFound + wrongValsFound);
//        if (correctValsFound + wrongValsFound == 0) {
//            precision = 0;
//        }
//        double recall = correctValsFound * 1.0 / totalCorrectVals;
//
//        // System.out.println(correctValsFound + "\t" + wrongValsFound + "\t" +
//        // totalCorrectVals);
//        return new Pair<Double, Double>(precision, recall);
//    }

//    public void testLuceneQuery() throws Exception {
//        // Init index
//        IndexReader ir = DirectoryReader.open(FSDirectory.open(new File("/home/jmorcos/WT/dwtcIndex").toPath()));
//        IndexSearcher is = new IndexSearcher(ir);
//        // QueryParser qp = new QueryParser("contents", new
//        // StandardAnalyzer());
//        ComplexPhraseQueryParser qp = new ComplexPhraseQueryParser("contents",
//                new StandardAnalyzer());
//
//        qp.setFuzzyMinSim(1.0f);
//
//        Query q = qp
//                .parse("contents: (2014~1.0 Brazil~1.0 2018~1.0 Russia~1.0 2022~1.0 Qatar~1.0) title: (world cup) context:(world cup)");
//
//        TopDocs results = is.search(q, 1000);
//
//        ScoreDoc[] hits = results.scoreDocs;
//
//        for (ScoreDoc scoreDoc : hits) {
//            Document doc = ir.document(scoreDoc.doc);
//
//            System.out.println(scoreDoc.score + "\t" + doc.get("id") + "\t" + doc.get("title"));
//        }
//
//        ir.close();
//
//    }


//    public void testTane() throws Exception {
//        File dir = new File("/home/jmorcos/webtables/sample");
//
//        for (File fname : dir.listFiles(new FilenameFilter() {
//
//            @Override
//            public boolean accept(File dir, String name) {
//                return name.endsWith("test.csv");
//            }
//        })) {
//            Table t = new Table(fname.getCanonicalPath());
//
//            System.out.println(t);
//
//            TANEjava tane = new TANEjava(t, 0.1);
//
//            for (FunctionalDependency fd : tane.getFD()) {
//                System.out.println(fd);
//            }
//
//        }
//
//    }

//    public void testOpenCalais() throws Exception {
//        CalaisClient calaisClient = new CalaisRestClient("n7c5yyrz337hsfyabj7sd2ug");
//
//        CalaisResponse calaisResponse = calaisClient.analyze("Microsoft");
//
//        for (CalaisObject entity : calaisResponse.getEntities()) {
//            System.out.println("============================================");
//            for (String field : entity.getFieldNames()) {
//                System.out.println(field + "\t:\t" + entity.getField(field));
//            }
//            System.out.println("********************************************");
//
//        }
//
//        // System.out.println(TWTMain.getEntityFromCalais("ITALIA"));
//        // System.out.println(TWTMain.getEntityFromCalais("ITALY"));
//    }

//    public void createTestset() throws Exception {
//        BufferedWriter bw = new BufferedWriter(new FileWriter("langDetectTest.txt"));
//
//        //DetectorFactory.loadProfile("lib/languageDetection/profiles");
//
//        File srcDir = new File("/home/jmorcos/webtables/langSample");
//
//        JFrame frame = new JFrame("LangDetectTest");
//        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//
//        JTable tablePane = new JTable();
//
//        // JButton yesButton = new JButton("English");
//        // JButton noButton = new JButton("Non-English");
//        //
//        // JPanel btnPanel = new JPanel();
//        // btnPanel.add(yesButton);
//        // btnPanel.add(noButton);
//
//        frame.add(tablePane);
//
//        ArrayList<String> files = new ArrayList<>();
//        for (String fname : srcDir.list(new FilenameFilter() {
//            @Override
//            public boolean accept(File dir, String name) {
//                return name.endsWith(".csv");
//            }
//        })) {
//
//            files.add(srcDir.getCanonicalPath() + File.separatorChar + fname);
//        }
//
//        for (String file : files) {
//
//            Table table = new Table(file);
//
//            // System.out.println(table);
//
//            DefaultTableModel tableModel = new DefaultTableModel();
//            for (int i = 0; i < table.getColumnMapping().getColumnNames().length; i++) {
//                tableModel.addColumn(table.getColumnMapping().getColumnNames()[i]);
//            }
//
//            for (Tuple tuple : table.getTuples()) {
//                String[] rowData = new String[tuple.getCells().size()];
//
//                for (int j = 0; j < rowData.length; j++) {
//                    rowData[j] = tuple.getCell(j).getValue();
//                }
//
//                tableModel.addRow(rowData);
//            }
//
//            tablePane.setModel(tableModel);
//
//            frame.pack();
//            frame.repaint();
//            frame.setVisible(true);
//
//            int ans = JOptionPane.showOptionDialog(frame, "Is this English?", "Is this English?",
//                    JOptionPane.DEFAULT_OPTION, JOptionPane.PLAIN_MESSAGE, null, new Object[]{
//                            "Yes", "No", "Skip", "Cancel"}, null);
//
//            if (ans == 0) {
//                bw.write(file + "," + 1 + "\n");
//            } else if (ans == 1) {
//                bw.write(file + "," + 0 + "\n");
//            } else if (ans == 2) {
//                continue; // skip: for 1-D tables
//            } else {
//                break;
//            }
//
//        }
//
//        bw.flush();
//        bw.close();
//
//    }

//    public void measureQuality() throws Exception {
//        //DetectorFactory.loadProfile("lib/languageDetection/profiles");
//
//        int tp = 0;
//        int tn = 0;
//        int fp = 0;
//        int fn = 0;
//
//        BufferedReader br = new BufferedReader(new FileReader("langDetectTest.txt"));
//        String line = null;
//        while ((line = br.readLine()) != null) {
//            String file = line.split(",")[0];
//            int ans = Integer.parseInt(line.split(",")[1]);
//
//            FileReader fr = new FileReader(file);
//            Detector detector = DetectorFactory.create();
//            detector.append(fr);
//            fr.close();
//
//            String lang = detector.detect();
//
//            if (lang.equals("en")) {
//                if (ans == 1) {
//                    tp++;
//                } else {
//                    fp++;
//                }
//            } else {
//                if (ans == 1) {
//                    fn++;
//                } else {
//                    tn++;
//                }
//            }
//
//        }
//        System.out.println("Precision = " + (tp * 1.0 / (tp + fp)));
//        System.out.println("Recall = " + (tp * 1.0 / (tp + fn)));
//        System.out.println("Eng percent " + ((tp + fn) * 1.0 / (tp + tn + fp + fn)));
//        System.out.println("Total is " + (tp + tn + fp + fn) + " files");
//
//    }

//    public void createInternalGroundTruth() throws SQLException, IOException, ParseException,
//            InterruptedException, ExecutionException, ClassNotFoundException {
//        Table fullTable = new Table("/home/jmorcos/webtables/data/zips_external.csv");
//        int[] colsFrom = new int[]{0, 2};
//        int[] colsTo = new int[]{5};
//
//        CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(new FileOutputStream(
//                "/home/jmorcos/zips_temp2.csv"), "UTF-8"), ',', '"');
//
//        csvWriter.writeNext(fullTable.getColumnMapping().getColumnNames());
//
//        final Table internalTable = new Table(fullTable, false);
//        Class.forName("com.vertica.jdbc.Driver");
//        VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection(
//                verticaProperties.getProperty("database_host") + verticaProperties.getProperty("database"), verticaProperties);
////        VerticaConnection con = (VerticaConnection) java.sql.DriverManager.getConnection(
////                databaseHost + verticaDatabase, verticaUser, verticaPassword);
//
//        MultiColVerticaQuerier mcvq = new MultiColVerticaQuerier(con);
//
//        int i = 0;
//        for (Tuple tuple : fullTable.getTuples()) {
//
//            final List<String> k = tuple.getValuesOfCells(colsFrom);
//            final List<String> v = tuple.getValuesOfCells(colsTo);
//
//            StemMultiColMap<StemMultiColHistogram> knownExamples = new StemMultiColMap<>();
//
//            StemMultiColHistogram hs = new StemMultiColHistogram();
//            hs.increment(v);
//            knownExamples.put(k, hs);
//
//            ArrayList<MultiColMatchCase> results = mcvq.findTables(knownExamples, 1);
//            if (results.size() > 0) {
//                // exists
//                internalTable.addTuple(new Tuple(tuple));
//
//                String[] tupVals = new String[tuple.getCells().size()];
//                for (int j = 0; j < tupVals.length; j++) {
//                    tupVals[j] = tuple.getCell(j).getValue();
//                }
//                csvWriter.writeNext(tupVals);
//                csvWriter.flush();
//            }
//
//            i++;
//
//            if (i % 100 == 0) {
//                System.out.println("Finished " + i + " tuples out of " + fullTable.getNumRows());
//            }
//        }
//        con.close();
//        csvWriter.close();
//
//        internalTable.saveToCSVFile("/home/jmorcos/webtables/data/zips_internal.csv");
//
//    }

//    public void crawlExternalResults() throws Exception {
//
//        int numberOfCrawlers = 7;
//
//        CrawlConfig config = new CrawlConfig();
//        config.setCrawlStorageFolder("/home/jmorcos/webtables/testCrawling");
//
//        /*
//         * Instantiate the controller for this crawl.
//         */
//        PageFetcher pageFetcher = new PageFetcher(config);
//        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
//        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
//        CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);
//
//        /*
//         * For each crawl, you need to add some seed urls. These are the first
//         * URLs that are fetched and then the crawler starts following links
//         * which are found in these pages
//         */
//        controller.addSeed("http://www.dec.ny.gov/pubs/grants.html");
//
//        /*
//         * Start the crawl. This is a blocking operation, meaning that your code
//         * will reach the line after this only when crawling is finished.
//         */
//        controller.start(Crawler.class, numberOfCrawlers);
//    }
}
