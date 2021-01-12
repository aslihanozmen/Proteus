package main;

import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.transformer.precisionRecall.FunctionalIndirect;
import model.BatchTableLoader;
import model.IntermediateTable;
import model.Table;
import model.Tuple;
import model.VerticaBatchTableLoader;
import model.multiColumn.MultiColTableLoader;

import org.apache.lucene.queryparser.classic.ParseException;

import org.magicwerk.brownies.collections.GapList;
import query.multiColumn.MultiColTableQuerier;
import query.multiColumn.VerticaMultiColSimilarityQuerier;
import test.IndirectMatchingTest;
import util.Histogram;
import util.MultiColMatchCase;
import util.MultiColMatchCaseComparator;
import util.Pair;
import util.Similarity;
import util.StemMultiColHistogram;
import util.StemMultiColMap;
import util.fd.tane.FunctionalDependency;
import util.fd.tane.TANEjava;

/**
 * @author John Morcos Supports set semantics, where each value is also
 * composite
 */
public class WTTransformerIndirect {
    public static final int MAX_THREADS = Runtime.getRuntime()
            .availableProcessors();
    public static int COVERAGE_THRESHOLD = 2;
    private static final double QUALITY_THRESHOLD = 0.3;
    public boolean fuzzyMatching = false;
    public static double schemaMatchPrior = 0.5;
    public static double schemaMatchWeight = 1;

    public enum ConsolidationMethod {
        NON_FUNCTIONAL, FUNCTIONAL, MAJORITY
    }

    private ConsolidationMethod consolidation = ConsolidationMethod.FUNCTIONAL;

    public enum ValidationExamplesSelectionPolicy {
        X_MAX_NUM_TABLES, XY_MAX_NUM_TABLES, XY_MIN_NUM_TABLES, XY_ENTROPY, XY_MAX_COVER, XY_MIN_OVERLAP,
        /**
         * This one is for debugging/experiments purposes only. Assumes
         * GeneralTests.groundTruth is populated
         */
        ORACLE_BAD_EXAMPLES
    }

    private StemMultiColMap<StemMultiColMap<Boolean>> validatedExamples = new StemMultiColMap<>();
    public static final double BAD_EXAMPLE_WEIGHT = 10.0;

    /**
     * Those 2 are for progress report
     */
    public String message = null;
    public boolean done = false;
    /*********************************/

    // private static final String ROOT_TABLE_CODE = "root";
    private static final double FD_ERROR = 0.10;

    private static final double SMOOTHING_FACTOR_ALPHA = 0.99;
    public static final MultiColMatchCase QUERY_TABLE_ID = new MultiColMatchCase(
            "query", new int[]{0}, new int[]{1});

    //TODO It was 100 before Asli!
    public static final int MAX_ITERS_FOR_EM = 2;

    private ConcurrentHashMap<MultiColMatchCase, Double> tableToRating = new ConcurrentHashMap<MultiColMatchCase, Double>();
    private ConcurrentHashMap<MultiColMatchCase, Double> tableToPrior = new ConcurrentHashMap<MultiColMatchCase, Double>();
    private ConcurrentHashMap<MultiColMatchCase, Double> tableToSchemaScore = new ConcurrentHashMap<MultiColMatchCase, Double>();
    private ConcurrentHashMap<MultiColMatchCase, HashSet<Pair<List<String>, List<String>>>> tableToAnswers = new ConcurrentHashMap<>();
    private double initialGoodExampleImportance = 1;
    public static boolean originalOnly = false;

    public static boolean verbose = true;

    private Table inputTable = null;
    public static TIntObjectHashMap<String> idToExternalId = new TIntObjectHashMap<>();
    public boolean toConvergence = true;
    private StemMultiColMap<StemMultiColHistogram> knownExamples = new StemMultiColMap<StemMultiColHistogram>();
    /**
     * These are the examples KNOWN to be bad (user-specified)
     */
    private StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> badExamples = new StemMultiColMap<>();

    /**
     * Used for fuzzy matching
     */
    private HashMap<List<String>, Collection<List<String>>> exactOriginalExamples = new HashMap<>();

    private StemMultiColMap<StemMultiColHistogram> answerToRating = new StemMultiColMap<StemMultiColHistogram>();
    private StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImages = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();

    public StemMultiColMap<StemMultiColHistogram> getAllAnswerRatings() {
        return answerToRating;
    }

    // State information for iterations
    private int[] colsFrom;
    private int[] colsTo;
    private MultiColTableQuerier queryBuilder;
    private MultiColTableLoader tableLoader;
    private boolean closedWorld;
    private boolean useWeightedExamples;
    private boolean localCompleteness;
    private int inductiveIters;
    private boolean useNewlyDiscoveredKeys;
    private boolean augment;

    public WTTransformerIndirect() {
    }

    /**
     * @param inputTable
     * @param consolidationMethod
     * @param colsFrom
     * @param colsTo
     * @param queryBuilder
     * @param tableLoader
     * @param closedWorld
     * @param useWeightedExamples
     * @param localCompleteness
     * @param inductiveIters
     * @param keywords
     * @param useNewlyDiscoveredKeys
     * @param augment
     * @return
     * @throws IOException
     * @throws ParseException
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws SQLException
     * @throws FileNotFoundException
     */
    public WTTransformerIndirect(Table inputTable,
                                 ConsolidationMethod consolidationMethod, int[] colsFrom,
                                 int[] colsTo, MultiColTableQuerier queryBuilder,
                                 final MultiColTableLoader tableLoader, final boolean closedWorld,
                                 final boolean useWeightedExamples, final boolean localCompleteness,
                                 int inductiveIters, String[] keywords,
                                 final boolean useNewlyDiscoveredKeys, boolean augment, String inputCSVName) {
        this.inputTable = inputTable;
        this.consolidation = consolidationMethod;
        this.colsFrom = colsFrom;
        this.colsTo = colsTo;
        this.queryBuilder = queryBuilder;
        this.tableLoader = tableLoader;
        this.closedWorld = closedWorld;
        this.useWeightedExamples = useWeightedExamples;
        this.localCompleteness = localCompleteness;
        this.inductiveIters = inductiveIters;
        this.useNewlyDiscoveredKeys = useNewlyDiscoveredKeys;
        this.augment = augment;

        for (Tuple t : inputTable.getTuples()) {
            List<String> k = t.getValuesOfCells(colsFrom);

            keyToImages.put(k,
                    new StemMultiColMap<HashSet<MultiColMatchCase>>());
        }

        tableToAnswers.put(QUERY_TABLE_ID,
                new HashSet<Pair<List<String>, List<String>>>());

        for (Tuple tuple : inputTable.getTuples()) {
            if (!isNull(tuple.getValuesOfCells(colsTo))) {
                List<String> k = tuple.getValuesOfCells(colsFrom);
                List<String> v = tuple.getValuesOfCells(colsTo);

                if (!knownExamples.containsKey(k)) {
                    knownExamples.put(k, new StemMultiColHistogram());
                    validatedExamples.put(k, new StemMultiColMap<Boolean>());
                }

                knownExamples.get(k).increment(v, initialGoodExampleImportance);
                validatedExamples.get(k).put(v, true);

                exactOriginalExamples.put(k, new HashSet<List<String>>());

                HashSet<MultiColMatchCase> evidence = new HashSet<MultiColMatchCase>();
                evidence.add(QUERY_TABLE_ID);

                keyToImages.get(k).put(v, evidence);

                exactOriginalExamples.get(k).add(v);
                tableToAnswers.get(QUERY_TABLE_ID).add(
                        new Pair<List<String>, List<String>>(k, v));
            }
        }
    }

    // private static HashMap<String, Tuple> valueToVirtualTuple;
    // private static Table virtualTable;

    public void setInductiveIters(int indicutiveIters) {
        this.inductiveIters = indicutiveIters;
    }

    public void setQueryBuilder(MultiColTableQuerier queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public void setTableLoader(MultiColTableLoader tableLoader) {
        this.tableLoader = tableLoader;
    }

    public void iterate() throws IOException, ParseException,
            InterruptedException, ExecutionException, SQLException {

        long startTableExtraction = System.currentTimeMillis();

        HashSet<MultiColMatchCase> seenTriples = new HashSet<MultiColMatchCase>();
        int iter = 0;
        StemMultiColMap<StemMultiColHistogram> lastKnownExamples = new StemMultiColMap<StemMultiColHistogram>();
        StemMultiColMap<String> foundXs = new StemMultiColMap<String>();

        boolean doQueries = iter < inductiveIters;// flag that checks

        done = false;
        message = "Initializing";
        int numberOfExamples = knownExamples.size();
        answerToRating.putAll(knownExamples);
        while (true) {
            // Clone knownExamples
            lastKnownExamples = cloneExamples(lastKnownExamples);

            StemMultiColMap<String> newlyFoundXs = new StemMultiColMap<String>();

            if (doQueries) {
                message = "Iteration " + (iter + 1) + " - querying for tables";

                System.out.println("Provided Examples: " + knownExamples);

                ArrayList<MultiColMatchCase> tablesToCheck = queryBuilder
                        .findTables(knownExamples, COVERAGE_THRESHOLD);

                loadTables(tablesToCheck);
                // TODO collect table mapping results.
                message = "Iteration " + (iter + 1) + " - checking tables";
                System.out.println(tablesToCheck.size() + " Tables to check");
                ExecutorService threadPool = Executors
                        .newFixedThreadPool(MAX_THREADS);
                ArrayList<TableChecker> tableCheckers = new ArrayList<WTTransformerIndirect.TableChecker>();

				long start = System.currentTimeMillis();
				for (final MultiColMatchCase MultiColMatchCase : tablesToCheck) {
					if (seenTriples.contains(MultiColMatchCase)) {
						continue;
					} else {
						if (originalOnly) {
							Table webTable = tableLoader.loadTable(MultiColMatchCase);
							if (webTable.source != 1) {
								continue;
							}
						}

                        seenTriples.add(MultiColMatchCase);
                        TableChecker tc = new TableChecker(keyToImages,
                                tableLoader, MultiColMatchCase,
                                useNewlyDiscoveredKeys, closedWorld,
                                useWeightedExamples, localCompleteness);
                        tableCheckers.add(tc);
                        threadPool.submit(tc);
                    }

                }

                threadPool.shutdown();
                awaitThreadPoolterminations(5, threadPool);

                message = "Iteration " + (iter + 1)
                        + " - consolidating results";
                System.out.println(message);

                collectResultsFromTableChecker(tableCheckers, foundXs,
                        newlyFoundXs);

                double secs = (System.currentTimeMillis() - start) * 1.0 / 1000;
                System.out.println("Finished checking tables, time = " + secs);
            }

            // Maximization: revise table ratings
            maximizationStep();
           // File dir = createDirectory("Benchmark_semantic_25", "/bestRatedTable/3");
//            for(Map.Entry<MultiColMatchCase, Double> entry : tableToRating.entrySet()) {
//                MultiColMatchCase table = entry.getKey();
//                if(!entry.getValue().equals(0.0) && !table.getTableID().equals("query")) {
//                    System.out.println("After checking tables: " + entry.getKey());
//                    //createWebTableCsv(dir, table);
//                }
//            }
            message = "Iteration " + (iter + 1) + " - adjusting answer scores";

            // Expectation: assess the scores and update examples
            knownExamples = new StemMultiColMap<>();
            answerToRating = new StemMultiColMap<>();

            ArrayList<Future<Pair<List<String>, StemMultiColHistogram>>> futures = new ArrayList<Future<Pair<List<String>, StemMultiColHistogram>>>(
                    keyToImages.size());
            ExecutorService es = Executors.newFixedThreadPool(MAX_THREADS);

            for (final List<String> k : keyToImages.keySet()) {
                if (keyToImages.get(k).size() > 0) {
                    futures.add(es
                            .submit(new Callable<Pair<List<String>, StemMultiColHistogram>>() {
                                @Override
                                public Pair<List<String>, StemMultiColHistogram> call() {

                                    StemMultiColHistogram distrib = computeAnswerScores(keyToImages
                                            .get(k));
                                    return new Pair<List<String>, StemMultiColHistogram>(
                                            k, distrib);

                                }
                            }));
                }
            }

            es.shutdown();
            awaitThreadPoolterminations(1, es);

            for (Future<Pair<List<String>, StemMultiColHistogram>> future : futures) {
                List<String> k = future.get().key;
                StemMultiColHistogram distrib = future.get().value;
                Pair<List<String>, Double> bestEntry = distrib
                        .getCountsSorted().get(0);
                double maxScore = bestEntry.value;
                if (consolidation == ConsolidationMethod.FUNCTIONAL) {
                    StemMultiColHistogram s = new StemMultiColHistogram();
                    s.increment(bestEntry.key, bestEntry.value);
                    knownExamples.put(k, s);
//					System.out.println(k + " -> " + bestEntry.key + " "
//							+ maxScore);
                } else {
                    knownExamples.put(k, distrib);
                }
                answerToRating.put(k, distrib);
            }

            // Check for convergence
            if (!doQueries) {
                double errorSum = computeErrorSum(lastKnownExamples);
                if (errorSum < 1e-5) {
                    break;
                }
            }


            ++iter;
            if (iter >= MAX_ITERS_FOR_EM) {
                break;
            }

            if (iter >= inductiveIters || newlyFoundXs.size() == 0) {
                if (doQueries) // print only on the first iter we stop querying
                {
                    if (newlyFoundXs.size() == 0) {
                        System.out
                                .println("Can't cover more X's...no more queries");
                    }

                    if (iter >= inductiveIters) {
                        System.out
                                .println("Reached the maximum allowed number of querying iterations");
                    }
                }
                doQueries = false;

                message = "Can't cover more X's...no more queries to the corpus";
                if (!toConvergence) {
                    System.out.println("Convergence disabled. Ending now");
                    break;
                }
            }
        }
        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> emptyResults = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();
        Pair<Double, Double> pr = IndirectMatchingTest.precisionRecall(this, IndirectMatchingTest.groundTruth, emptyResults, numberOfExamples);
        System.out.println("final direct matches, " + pr.key + ", " + pr.value);
        done = true;
        try {
            // find all table concatenations here
            StemMultiColMap<String> newlyFoundXs = new StemMultiColMap<String>();
            List<IntermediateTable> intermediateTables = findIntermediateMappings(new ArrayList<MultiColMatchCase>(
                    seenTriples));
            ExecutorService threadPool = Executors
                    .newFixedThreadPool(MAX_THREADS);
            ArrayList<TablePathChecker> tableCheckers = new ArrayList<WTTransformerIndirect.TablePathChecker>();
            for (IntermediateTable iTable : intermediateTables) {
                List<Pair<Table, MultiColMatchCase>> tables = iTable
                        .getzyTables();
                Table xzTable = iTable.getXzTable();
                HashMap<List<String>, Histogram<List<String>>> xzTree = createExactMapFromTable(
                        xzTable, iTable.getxColumns(), iTable.getzColumns());

                for (Pair<Table, MultiColMatchCase> tablePair : tables) {
                    TablePathChecker tc = new TablePathChecker(keyToImages,
                            tablePair, xzTable, iTable.getxColumns(), xzTree, useNewlyDiscoveredKeys,
                            closedWorld, useWeightedExamples, localCompleteness);
                    tableCheckers.add(tc);
                    threadPool.submit(tc);
                }

                threadPool.shutdown();
                awaitThreadPoolterminations(5, threadPool);

                message = "Iteration " + (iter + 1)
                        + " - consolidating results";
                System.out.println(message);

                collectResultsFromTablePathChecker(tableCheckers, foundXs,
                        newlyFoundXs);
                System.out.println("Finished checking joinedtables, time ");
                maximizationStep();

               // File dir = createDirectory("BenchmarkCUSIPToTicker", "/bestRatedTable/3");
//                for(Map.Entry<MultiColMatchCase, Double> entry : tableToRating.entrySet()) {
//                    MultiColMatchCase table = entry.getKey();
//                    if(!entry.getValue().equals(0.0) && !table.getTableID().equals("query")) {
//                        System.out.println("After finishing joined tables: " + entry.getKey());
//                        //createWebTableCsv(dir, table);
//                    }
//                }

                long end = System.currentTimeMillis();
                System.out.println("Table Extraction: " + (end - startTableExtraction));

                knownExamples = new StemMultiColMap<>();
                answerToRating = new StemMultiColMap<>();

                ArrayList<Future<Pair<List<String>, StemMultiColHistogram>>> futures = new ArrayList<Future<Pair<List<String>, StemMultiColHistogram>>>(
                        keyToImages.size());
                ExecutorService es = Executors.newFixedThreadPool(MAX_THREADS);

                for (final List<String> k : keyToImages.keySet()) {
                    if (keyToImages.get(k).size() > 0) {
                        futures.add(es
                                .submit(new Callable<Pair<List<String>, StemMultiColHistogram>>() {
                                    @Override
                                    public Pair<List<String>, StemMultiColHistogram> call() {

                                        StemMultiColHistogram distrib = computeAnswerScores(keyToImages
                                                .get(k));
                                        return new Pair<List<String>, StemMultiColHistogram>(
                                                k, distrib);

                                    }
                                }));
                    }
                }

                es.shutdown();
                awaitThreadPoolterminations(1, es);

                for (Future<Pair<List<String>, StemMultiColHistogram>> future : futures) {
                    List<String> k = future.get().key;
                    StemMultiColHistogram distrib = future.get().value;
                    Pair<List<String>, Double> bestEntry = distrib
                            .getCountsSorted().get(0);
                    double maxScore = bestEntry.value;
                    if (consolidation == ConsolidationMethod.FUNCTIONAL) {
                        StemMultiColHistogram s = new StemMultiColHistogram();
                        s.increment(bestEntry.key, bestEntry.value);
                        knownExamples.put(k, s);
                        System.out.println(k + " -> " + bestEntry.key + " "
                                + maxScore);
                    } else {
                        knownExamples.put(k, distrib);
                    }
                    answerToRating.put(k, distrib);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void iterateSyntactic() throws Exception {

        long startTableExtraction = System.currentTimeMillis();

        HashSet<MultiColMatchCase> seenTriples = new HashSet<MultiColMatchCase>();
        int iter = 0;
        StemMultiColMap<StemMultiColHistogram> lastKnownExamples = new StemMultiColMap<StemMultiColHistogram>();
        StemMultiColMap<String> foundXs = new StemMultiColMap<String>();

        boolean doQueries = iter < inductiveIters;// flag that checks

        done = false;
        message = "Initializing";
        int numberOfExamples = knownExamples.size();
        answerToRating.putAll(knownExamples);
        while (true) {
            // Clone knownExamples
            lastKnownExamples = cloneExamples(lastKnownExamples);

            StemMultiColMap<String> newlyFoundXs = new StemMultiColMap<String>();

            if (doQueries) {
                message = "Iteration " + (iter + 1) + " - querying for tables";

                System.out.println("Provided Examples: " + knownExamples);

                ArrayList<MultiColMatchCase> tablesToCheck = queryBuilder
                        .findTables(knownExamples, COVERAGE_THRESHOLD);

                loadTables(tablesToCheck);
                // TODO collect table mapping results.
                message = "Iteration " + (iter + 1) + " - checking tables";
                System.out.println(tablesToCheck.size() + " Tables to check");
                ExecutorService threadPool = Executors
                        .newFixedThreadPool(MAX_THREADS);
                ArrayList<TableChecker> tableCheckers = new ArrayList<WTTransformerIndirect.TableChecker>();

                long start = System.currentTimeMillis();
                for (final MultiColMatchCase MultiColMatchCase : tablesToCheck) {
                    if (seenTriples.contains(MultiColMatchCase)) {
                        continue;
                    } else {
                        if (originalOnly) {
                            Table webTable = tableLoader.loadTable(MultiColMatchCase);
                            if (webTable.source != 1) {
                                continue;
                            }
                        }

                        seenTriples.add(MultiColMatchCase);
                        TableChecker tc = new TableChecker(keyToImages,
                                tableLoader, MultiColMatchCase,
                                useNewlyDiscoveredKeys, closedWorld,
                                useWeightedExamples, localCompleteness);
                        tableCheckers.add(tc);
                        threadPool.submit(tc);
                    }

                }

                threadPool.shutdown();
                awaitThreadPoolterminations(5, threadPool);

                message = "Iteration " + (iter + 1)
                        + " - consolidating results";
                System.out.println(message);

                collectResultsFromTableChecker(tableCheckers, foundXs,
                        newlyFoundXs);

                double secs = (System.currentTimeMillis() - start) * 1.0 / 1000;
                System.out.println("Finished checking tables, time = " + secs);
            }

            // Maximization: revise table ratings
            maximizationStep();
            File dir = createDirectory("experiment", "/bestRatedTable");
            for(Map.Entry<MultiColMatchCase, Double> entry : tableToRating.entrySet()) {
                MultiColMatchCase table = entry.getKey();
                if(!entry.getValue().equals(0.0) && !table.getTableID().equals("query")) {
                    System.out.println("After checking tables: " + entry.getKey());
                    createIfNotExist(dir, table);
                }
            }
            message = "Iteration " + (iter + 1) + " - adjusting answer scores";

            // Expectation: assess the scores and update examples
            knownExamples = new StemMultiColMap<>();
            answerToRating = new StemMultiColMap<>();

            ArrayList<Future<Pair<List<String>, StemMultiColHistogram>>> futures = new ArrayList<Future<Pair<List<String>, StemMultiColHistogram>>>(
                    keyToImages.size());
            ExecutorService es = Executors.newFixedThreadPool(MAX_THREADS);

            for (final List<String> k : keyToImages.keySet()) {
                if (keyToImages.get(k).size() > 0) {
                    futures.add(es
                            .submit(new Callable<Pair<List<String>, StemMultiColHistogram>>() {
                                @Override
                                public Pair<List<String>, StemMultiColHistogram> call() {

                                    StemMultiColHistogram distrib = computeAnswerScores(keyToImages
                                            .get(k));
                                    return new Pair<List<String>, StemMultiColHistogram>(
                                            k, distrib);

                                }
                            }));
                }
            }

            es.shutdown();
            awaitThreadPoolterminations(1, es);

            for (Future<Pair<List<String>, StemMultiColHistogram>> future : futures) {
                List<String> k = future.get().key;
                StemMultiColHistogram distrib = future.get().value;
                Pair<List<String>, Double> bestEntry = distrib
                        .getCountsSorted().get(0);
                double maxScore = bestEntry.value;
                if (consolidation == ConsolidationMethod.FUNCTIONAL) {
                    StemMultiColHistogram s = new StemMultiColHistogram();
                    s.increment(bestEntry.key, bestEntry.value);
                    knownExamples.put(k, s);
//					System.out.println(k + " -> " + bestEntry.key + " "
//							+ maxScore);
                } else {
                    knownExamples.put(k, distrib);
                }
                answerToRating.put(k, distrib);
            }

            // Check for convergence
            if (!doQueries) {
                double errorSum = computeErrorSum(lastKnownExamples);
                if (errorSum < 1e-5) {
                    break;
                }
            }


            ++iter;
            if (iter >= MAX_ITERS_FOR_EM) {
                break;
            }

            if (iter >= inductiveIters || newlyFoundXs.size() == 0) {
                if (doQueries) // print only on the first iter we stop querying
                {
                    if (newlyFoundXs.size() == 0) {
                        System.out
                                .println("Can't cover more X's...no more queries");
                    }

                    if (iter >= inductiveIters) {
                        System.out
                                .println("Reached the maximum allowed number of querying iterations");
                    }
                }
                doQueries = false;

                message = "Can't cover more X's...no more queries to the corpus";
                if (!toConvergence) {
                    System.out.println("Convergence disabled. Ending now");
                    break;
                }
            }
        }
        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> emptyResults = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();
        Pair<Double, Double> pr = IndirectMatchingTest.precisionRecall(this, IndirectMatchingTest.groundTruth, emptyResults, numberOfExamples);
        System.out.println("final direct matches, " + pr.key + ", " + pr.value);
        done = true;
        try {
            // find all table concatenations here
            StemMultiColMap<String> newlyFoundXs = new StemMultiColMap<String>();
            List<IntermediateTable> intermediateTables = findIntermediateMappings(new ArrayList<MultiColMatchCase>(
                    seenTriples));
            ExecutorService threadPool = Executors
                    .newFixedThreadPool(MAX_THREADS);
            ArrayList<TablePathChecker> tableCheckers = new ArrayList<WTTransformerIndirect.TablePathChecker>();
            for (IntermediateTable iTable : intermediateTables) {
                List<Pair<Table, MultiColMatchCase>> tables = iTable
                        .getzyTables();
                Table xzTable = iTable.getXzTable();
                HashMap<List<String>, Histogram<List<String>>> xzTree = createExactMapFromTable(
                        xzTable, iTable.getxColumns(), iTable.getzColumns());

                for (Pair<Table, MultiColMatchCase> tablePair : tables) {
                    TablePathChecker tc = new TablePathChecker(keyToImages,
                            tablePair, xzTable, iTable.getxColumns(), xzTree, useNewlyDiscoveredKeys,
                            closedWorld, useWeightedExamples, localCompleteness);
                    tableCheckers.add(tc);
                    threadPool.submit(tc);
                }

                threadPool.shutdown();
                awaitThreadPoolterminations(5, threadPool);

                message = "Iteration " + (iter + 1)
                        + " - consolidating results";
                System.out.println(message);

                collectResultsFromTablePathChecker(tableCheckers, foundXs,
                        newlyFoundXs);
                System.out.println("Finished checking joinedtables, time ");
                maximizationStep();

                File dir = createDirectory("experiment", "/bestRatedTable");
                for(Map.Entry<MultiColMatchCase, Double> entry : tableToRating.entrySet()) {
                    MultiColMatchCase table = entry.getKey();
                    if(!entry.getValue().equals(0.0) && !table.getTableID().equals("query")) {
                        System.out.println("After finishing joined tables: " + entry.getKey());
                        createIfNotExist(dir,table);
                    }
                }

                long end = System.currentTimeMillis();
                System.out.println("Table Extraction: " + (end - startTableExtraction));

                knownExamples = new StemMultiColMap<>();
                answerToRating = new StemMultiColMap<>();

                ArrayList<Future<Pair<List<String>, StemMultiColHistogram>>> futures = new ArrayList<Future<Pair<List<String>, StemMultiColHistogram>>>(
                        keyToImages.size());
                ExecutorService es = Executors.newFixedThreadPool(MAX_THREADS);

                for (final List<String> k : keyToImages.keySet()) {
                    if (keyToImages.get(k).size() > 0) {
                        futures.add(es
                                .submit(new Callable<Pair<List<String>, StemMultiColHistogram>>() {
                                    @Override
                                    public Pair<List<String>, StemMultiColHistogram> call() {

                                        StemMultiColHistogram distrib = computeAnswerScores(keyToImages
                                                .get(k));
                                        return new Pair<List<String>, StemMultiColHistogram>(
                                                k, distrib);

                                    }
                                }));
                    }
                }

                es.shutdown();
                awaitThreadPoolterminations(1, es);

                for (Future<Pair<List<String>, StemMultiColHistogram>> future : futures) {
                    List<String> k = future.get().key;
                    StemMultiColHistogram distrib = future.get().value;
                    Pair<List<String>, Double> bestEntry = distrib
                            .getCountsSorted().get(0);
                    double maxScore = bestEntry.value;
                    if (consolidation == ConsolidationMethod.FUNCTIONAL) {
                        StemMultiColHistogram s = new StemMultiColHistogram();
                        s.increment(bestEntry.key, bestEntry.value);
                        knownExamples.put(k, s);
                        System.out.println(k + " -> " + bestEntry.key + " "
                                + maxScore);
                    } else {
                        knownExamples.put(k, distrib);
                    }
                    answerToRating.put(k, distrib);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        FunctionalIndirect.precisionRecall("./experiment/tablesToCheck",
                false);

    }

    private void createIfNotExist(File dir, MultiColMatchCase table) throws IOException, SQLException {
        String tableId = table.tableID;
        String tableFull = table.toString();

        Set<String> columns = getColumnStrings(dir, tableId, tableFull);
        createWebTableCsvIndirect(dir,table,columns);
    }

    public Set<String> getColumnStrings(File dir, String tableId, String tableFull) {
        List<String> filenames = new GapList<>();
        Set<String> columns = new HashSet<>();
        for (final File fileEntry : Objects.requireNonNull(dir.listFiles())) {
            String fileName = fileEntry.getName();
            if(fileName.contains(tableId)) {
                Pattern p = Pattern.compile("(?<=\\[)([^]]+)(?=])");
                Matcher m = p.matcher(fileName);
                while(m.find()) {
                    String column =  m.group(1);
                    columns.add(column);
                }
                Matcher second = p.matcher(tableFull);
                while(second.find()) {
                    String column =  second.group(1);
                    columns.add(column);
                }
                fileEntry.delete();
            }
        }
        return columns;
    }

    public void createWebTableCsvIndirect(File dir, MultiColMatchCase table, Set<String> candidateKeys) throws IOException, SQLException {
        Table webTable = tableLoader.loadTable(table);
        String name = table.toString();
        String tableId = table.tableID;

        File file = getIndirectNames(dir, candidateKeys, name, tableId);

        webTable.saveToCSVFile(file.getAbsolutePath());
    }

    public File getIndirectNames(File dir, Set<String> candidateKeys, String name, String tableId) throws IOException {
        StringBuilder sb;
        if (!candidateKeys.isEmpty()) {
            sb = new StringBuilder("(").append(tableId);
            for(String key: candidateKeys) {
                sb.append(",").append("[").append(key).append("]");
            }
            name = sb.append(")").toString();
        }

        File file = new File(dir.getAbsolutePath() + "/" + name + ".csv");
        if (file.createNewFile()) {
            System.out.println("Recreating the file");
        } else {
            PrintWriter writer = new PrintWriter(file);
            writer.print("");
            writer.close();
        }
        return file;
    }


    public Table createWebTableCsv(File dir, MultiColMatchCase table) throws SQLException, IOException {
        Table webTable = tableLoader.loadTable(table);
        File file = new File(dir.getAbsolutePath() + "/" + table + ".csv");
        if (file.createNewFile()) {
            System.out.println("Recreating the file");
        } else {
            PrintWriter writer = new PrintWriter(file);
            writer.print("");
            writer.close();
        }
        webTable.saveToCSVFile(file.getAbsolutePath());
        return webTable;
    }

    private Collection<String>[] extractXs() {
        HashSet<String>[] xs = new HashSet[knownExamples.keySet().iterator()
                .next().size()];
        for (int i = 0; i < xs.length; i++) {
            xs[i] = new HashSet<String>();
        }
        for (List<String> x : knownExamples.keySet()) {
            for (int i = 0; i < xs.length; i++) {
                xs[i].add(x.get(i));
            }
        }
        return xs;
    }

    private Collection<String>[] extractMappingValues(
            StemMultiColMap<StemMultiColHistogram> knownExamples) {
        HashSet<String>[] ys = new HashSet[knownExamples.values().iterator()
                .next().getCountsUnsorted().keySet().iterator().next().size()];

        for (int i = 0; i < ys.length; i++) {
            ys[i] = new HashSet<String>();
        }
        for (List<String> x : knownExamples.keySet())
            for (List<String> y : knownExamples.get(x).getCountsUnsorted()
                    .keySet()) {
                for (int i = 0; i < ys.length; i++) {
                    ys[i].add(y.get(i));
                }
            }
        return ys;
    }

    private void collectResultsFromTableChecker(
            ArrayList<TableChecker> tableCheckers,
            StemMultiColMap<String> foundXs,
            StemMultiColMap<String> newlyFoundXs) {
        for (TableChecker tc : tableCheckers) {
            if (tc.tableToPriorPartial.size() > 0) {
                StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> partialResult = tc.result;
                tableToPrior.putAll(tc.tableToPriorPartial);
                tableToAnswers.putAll(tc.tableToAnswersPartial);
                tableToSchemaScore.putAll(tc.tableToSchemaPriorPartial);

                for (List<String> k : partialResult.keySet()) {
                    if (!keyToImages.containsKey(k)) {
                        if (!useNewlyDiscoveredKeys) {
                            continue;
                        }
                        keyToImages
                                .put(k,
                                        new StemMultiColMap<HashSet<MultiColMatchCase>>());
                    }
                    for (List<String> v : partialResult.get(k).keySet()) {
                        if (badExamples.containsKey(k)
                                && badExamples.get(k).containsKey(v)) {
                            // add all its witness tables
                            badExamples.get(k).get(v)
                                    .addAll(partialResult.get(k).get(v));
                        } else {
                            if (!keyToImages.get(k).containsKey(v)) {
                                keyToImages.get(k).put(v,
                                        new HashSet<MultiColMatchCase>());
                            }
                            keyToImages.get(k).get(v)
                                    .addAll(partialResult.get(k).get(v));
                        }

                    }

                    if (!foundXs.containsKey(k)) {
                        newlyFoundXs.put(k, "");
                        foundXs.put(k, "");
                    }
                }
            }
        }

    }

    private void collectResultsFromTablePathChecker(
            ArrayList<TablePathChecker> tableCheckers,
            StemMultiColMap<String> foundXs,
            StemMultiColMap<String> newlyFoundXs) {
        for (TablePathChecker tc : tableCheckers) {
            if (tc.tableToPriorPartial.size() > 0) {
                StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> partialResult = tc.result;
                tableToPrior.putAll(tc.tableToPriorPartial);
                tableToAnswers.putAll(tc.tableToAnswersPartial);
                tableToSchemaScore.putAll(tc.tableToSchemaPriorPartial);

                for (List<String> k : partialResult.keySet()) {
                    if (!keyToImages.containsKey(k)) {
                        if (!useNewlyDiscoveredKeys) {
                            continue;
                        }
                        keyToImages
                                .put(k,
                                        new StemMultiColMap<HashSet<MultiColMatchCase>>());
                    }
                    for (List<String> v : partialResult.get(k).keySet()) {
                        if (badExamples.containsKey(k)
                                && badExamples.get(k).containsKey(v)) {
                            // add all its witness tables
                            badExamples.get(k).get(v)
                                    .addAll(partialResult.get(k).get(v));
                        } else {
                            if (!keyToImages.get(k).containsKey(v)) {
                                keyToImages.get(k).put(v,
                                        new HashSet<MultiColMatchCase>());
                            }
                            keyToImages.get(k).get(v)
                                    .addAll(partialResult.get(k).get(v));
                        }

                    }

                    if (!foundXs.containsKey(k)) {
                        newlyFoundXs.put(k, "");
                        foundXs.put(k, "");
                    }
                }
            }
        }

    }

    private void loadTables(ArrayList<MultiColMatchCase> tablesToCheck)
            throws SQLException {
        if (tableLoader instanceof BatchTableLoader) {
            HashSet<String> tableIDs = new HashSet<String>();
            for (MultiColMatchCase MultiColMatchCase : tablesToCheck) {
                tableIDs.add(MultiColMatchCase.tableID);
            }
            ((BatchTableLoader) tableLoader).loadTables(tableIDs
                    .toArray(new String[0]));
        }

    }

    private ArrayList<MultiColMatchCase> tableSetDifference(
            ArrayList<MultiColMatchCase> table1,
            ArrayList<MultiColMatchCase> table2) {
        ArrayList<MultiColMatchCase> newXTables = new ArrayList<>();
        MultiColMatchCaseComparator comparator = new MultiColMatchCaseComparator();
        Collections.sort(table1, comparator);
        Collections.sort(table2, new MultiColMatchCaseComparator());
        for (int i = 0, j = 0; i < table1.size(); ) {
            if (j < table2.size()) {
                int comparison = comparator.compare(table1.get(i),
                        table2.get(j));
                if (comparison == 0) {
                    ++i;
                    ++j;
                } else if (comparison < 0) {// i is smaller than j
                    newXTables.add(table1.get(i));
                    ++i;
                } else {
                    ++j;
                }
            } else {
                newXTables.addAll(table1.subList(i, table1.size()));
                break;
            }

        }
        return newXTables;
    }

    private List<IntermediateTable> findIntermediateMappings(
            ArrayList<MultiColMatchCase> allTables) throws Exception {
        List<IntermediateTable> intermediateTables = new ArrayList<>();
        Set<List<String>> values = new HashSet<>();

        ArrayList<MultiColMatchCase> xTables = queryBuilder.findTables(
                extractXs(), COVERAGE_THRESHOLD);

        xTables = tableSetDifference(xTables, allTables);// find Set
        allTables.addAll(xTables);
        loadTables(xTables);
        for (MultiColMatchCase tableTriple : xTables) {
            Table table = tableLoader.loadTable(tableTriple);
            int[] xColumn = tableTriple.getCol1();
            ArrayList<FunctionalDependency> fds = findFDsWithColAsLHS(table,
                    xColumn[0]);


//            System.out.println("Intermediate table: " + tableTriple);
//            File dir = createDirectory("BenchmarkCUSIPToTicker", "/intermediate/3");
//            createWebTableCsv(dir, tableTriple);



            if (!fds.isEmpty()) {
                // create data structure for maintaining z to y mappings
                // TODO remove duplicates
                for (FunctionalDependency fd : fds) {
                    int[] ZIds = fd.getYIds().toArray();
                    IntermediateTable intermediateTable = new IntermediateTable(
                            tableTriple.tableID, xColumn, ZIds, table);
                    int foundExample = 0;
                    int maxExamples = knownExamples.size();
                    values.clear();
                    // check if is FD:

                    for (Tuple tup : table.getTuples()) {
                        List<String> x = tup.getValuesOfCells(xColumn);
                        if (knownExamples.containsKey(x)) {
                            List<String> z = tup.getValuesOfCells(ZIds); // the
                            if (z == null) {
                                continue;
                            }
                            List<String> y = knownExamples.get(x)
                                    .getCountsUnsorted().keySet().iterator()
                                    .next();
                            values.add(y);
                            ++foundExample;
                            intermediateTable.appendMapping(x, z, y);
                        }
                        if (foundExample >= maxExamples) {
                            break;
                        }
                    }
                    // check whether Z can map to Y
                    System.out.println("chosen FD: " + fd);
                    if (extractMappingValues(intermediateTable.getxToZMapping())[0]
                            .size() >= values.size()) {
                        ArrayList<MultiColMatchCase> tablesToCheck = queryBuilder
                                .findTables(intermediateTable.getZToYMapping(),
                                        Math.min(COVERAGE_THRESHOLD, values.size()));
                        tablesToCheck = tableSetDifference(tablesToCheck,
                                allTables);
                        findJoinableTables(table, intermediateTable,
                                tablesToCheck);
                        if (!intermediateTable.getzyTables().isEmpty()) {
                            intermediateTables.add(intermediateTable);
//                            System.out.println("First Intermediate Tables: " + intermediateTable);
//                             dir = createDirectory("BenchmarkCUSIPToTicker", "/intermediate1/3");
//                            createWebTableCsv(dir, tableTriple);

                            return intermediateTables;
                        }
                        // TODO iterate over Y to Z
                    }
                }
            }
        }
        System.out.println(" Intermediate Tables: " + intermediateTables);
        return intermediateTables;

    }

    public File createDirectory(String inputFileName, String s3) {
        File dir = new File("./experiment/tablesToCheck/" + inputFileName +  "/" + s3);
        if (!dir.exists()) {
            if (dir.mkdirs()) {
                System.out.println("Directory for benchmark is created");
            }
        }
        return dir;
    }

    private void findJoinableTables(Table table,
                                    IntermediateTable intermediateTable,
                                    ArrayList<MultiColMatchCase> tablesToCheck)
            throws FileNotFoundException, SQLException, IOException {
        // Auto-generated method stub
        loadTables(tablesToCheck);
        StemMultiColMap<StemMultiColHistogram> desiredMapping = intermediateTable
                .getZToYMapping();
        for (MultiColMatchCase tableTriple : tablesToCheck) {
            Table zyTable = tableLoader.loadTable(tableTriple);
            HashMap<List<String>, StemMultiColHistogram> zyTableMapping = createMapFromTable(
                    zyTable, tableTriple.getCol1(), tableTriple.getCol2());
            int positiveMappings = 0;
            int negativeMappings = 0;
            for (List<String> zValue : desiredMapping.keySet()) {
                List<String> desiredYValue = desiredMapping.get(zValue)
                        .getCountsUnsorted().keySet().iterator().next();
                if (!zyTableMapping.containsKey(zValue)) {
                    continue;
                }
                if (zyTableMapping.get(zValue).getCountsUnsorted()
                        .containsKey(desiredYValue)
                        && zyTableMapping.get(zValue).getCountsUnsorted()
                        .size() < 2) {
                    ++positiveMappings;
                } else {
                    ++negativeMappings;
                    break;
                }
            }
            if (positiveMappings >= COVERAGE_THRESHOLD && negativeMappings == 0) {
                intermediateTable.addZYTable(zyTable, tableTriple);// desiredMappings
                // might not
                // hold all
                // in
                // zyTable
                // print out found mappings:
                // for (k: inputTable.)
            }
        }

    }

    private double computeErrorSum(
            StemMultiColMap<StemMultiColHistogram> lastKnownExamples) {
        double errorSum = 0.0;
        for (List<String> k : knownExamples.keySet()) {
            for (List<String> v : knownExamples.get(k).getCountsUnsorted()
                    .keySet()) {
                double s2 = knownExamples.get(k).getScoreOf(v);

                double s1 = 0;
                if (lastKnownExamples.containsKey(k)) {
                    if (lastKnownExamples.get(k).getCountsUnsorted()
                            .containsKey(v)) {
                        s1 = lastKnownExamples.get(k).getScoreOf(v);
                    }
                }

                errorSum += Math.abs(s2 - s1);
            }
        }

        // For the other side
        for (List<String> k : lastKnownExamples.keySet()) {
            for (List<String> v : lastKnownExamples.get(k).getCountsUnsorted()
                    .keySet()) {
                double s1 = lastKnownExamples.get(k).getScoreOf(v);

                double s2 = 0;
                if (knownExamples.containsKey(k)) {
                    if (knownExamples.get(k).getCountsUnsorted().containsKey(v)) {
                        s2 = knownExamples.get(k).getScoreOf(v);
                    }
                }

                errorSum += Math.abs(s2 - s1);
            }
        }
        return errorSum;
    }

    private void maximizationStep() {
        tableToRating.put(QUERY_TABLE_ID, 1.0);
        for (MultiColMatchCase MultiColMatchCase : tableToAnswers.keySet()) {

            if (MultiColMatchCase.equals(QUERY_TABLE_ID)) {
                tableToRating.put(MultiColMatchCase, 1.0);
                continue;
            }

            double rating = computeRating(MultiColMatchCase);

            tableToRating.put(MultiColMatchCase, rating);
        }

    }

    private void awaitThreadPoolterminations(int i, ExecutorService threadPool) {
        while (true) {
            try {
                if (threadPool.awaitTermination(i, TimeUnit.HOURS)) {
                    break;
                }
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

    }

    private StemMultiColMap<StemMultiColHistogram> cloneExamples(
            StemMultiColMap<StemMultiColHistogram> lastKnownExamples) {
        for (List<String> k : knownExamples.keySet()) {
            StemMultiColHistogram s = new StemMultiColHistogram();
            for (List<String> v : knownExamples.get(k).getCountsUnsorted()
                    .keySet()) {
                s.increment(v, knownExamples.get(k).getScoreOf(v));
            }
            lastKnownExamples.put(k, s);
        }
        return lastKnownExamples;
    }

    public void runEM() throws IOException, ParseException,
            InterruptedException, ExecutionException, SQLException {
        int oldInductiveIters = inductiveIters;
        boolean oldToConvergence = toConvergence;

        this.setInductiveIters(0); // No querying
        this.toConvergence = true;

        iterate();

        this.setInductiveIters(oldInductiveIters);
        this.toConvergence = oldToConvergence;
    }

    /**
     * @return
     */
    public StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> getCurrentAnswer() {
        if (augment) {
            return keyToImages;
        } else {
            // Remove spurious ones
            StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImagesFinal = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();

            for (Tuple t : inputTable.getTuples()) {
                List<String> k = t.getValuesOfCells(colsFrom);
                keyToImagesFinal.put(k,
                        new StemMultiColMap<HashSet<MultiColMatchCase>>());

                if (knownExamples.containsKey(k)) {
                    if (consolidation == ConsolidationMethod.FUNCTIONAL
                            || consolidation == ConsolidationMethod.MAJORITY) {
                        if (knownExamples.get(k).getCountsUnsorted().size() > 0) {
                            List<String> v = knownExamples.get(k)
                                    .getCountsSorted().get(0).key;
                            keyToImagesFinal.get(k).put(v,
                                    keyToImages.get(k).get(v));
                        }
                    } else if (consolidation == ConsolidationMethod.NON_FUNCTIONAL) {
                        for (List<String> v : knownExamples.get(k)
                                .getCountsUnsorted().keySet()) {
                            keyToImagesFinal.get(k).put(v,
                                    keyToImages.get(k).get(v));
                        }
                    }
                }

            }
            return keyToImagesFinal;
        }
    }

    /**
     * Computes the rating of a table (i.e., a match case)
     *
     * @param MultiColMatchCase
     * @return
     */
    private double computeRating(MultiColMatchCase MultiColMatchCase) {
        double prior = tableToPrior.get(MultiColMatchCase);

        double goodSum = 0;
        double totalSum = 0;

        HashSet<List<String>> coveredKeys = new HashSet<>();

        for (Pair<List<String>, List<String>> entry : tableToAnswers
                .get(MultiColMatchCase)) {
            List<String> k = entry.key;
            List<String> v = entry.value;

            coveredKeys.add(k);

            if (knownExamples.containsKey(k)) {
                if (knownExamples.get(k).getCountsUnsorted().containsKey(v)) {
                    goodSum += knownExamples.get(k).getScoreOf(v);
                    totalSum += knownExamples.get(k).getScoreOf(v);
                } else {
                    totalSum += knownExamples.get(k).getTotalCount();
                    // totalSum +=
                    // knownExamples.get(k).getCountsSorted().get(0).value;
                }
            }

            if (badExamples.containsKey(k) && badExamples.get(k).containsKey(v)) {
                totalSum += BAD_EXAMPLE_WEIGHT;
            }

        }

        double unseenWeightSum = 0;
        for (List<String> k : knownExamples.keySet()) {
            if (!coveredKeys.contains(k)) {
                unseenWeightSum += knownExamples.get(k).getTotalCount();
                // unseenWeightSum++;
            }
        }
        double instanceBasedRating = (goodSum + prior * unseenWeightSum)
                * SMOOTHING_FACTOR_ALPHA / (totalSum + unseenWeightSum);

        double rating = instanceBasedRating;
        return rating;
    }

    private double computeSchemaMatch(String[] h1, String[] h2) {
        String[] headers1 = inputTable.getColumnMapping().getColumnNames(
                colsFrom);
        String[] headers2 = inputTable.getColumnMapping()
                .getColumnNames(colsTo);

        double s1 = Similarity.similarity(StemMultiColMap.stemList(headers1),
                StemMultiColMap.stemList(h1));
        double s2 = Similarity.similarity(StemMultiColMap.stemList(headers2),
                StemMultiColMap.stemList(h2));

        // return Math.max(schemaMatchPrior, (s1 + s2) / 2);

        if (s1 < 0.5) {
            s1 = 0;
        }
        if (s2 < 0.5) {
            s2 = 0;
        }
        return (s1 + s2) / 2;

    }

    /**
     * A wrapper for the original method, that returns only a single (the top)
     * answer for each key
     */
    public StemMultiColMap<Pair<List<String>, Double>> transformFunctional()
            throws IOException, ParseException, SQLException,
            InterruptedException, ExecutionException {
        iterate();// actually everything important happens in this method
        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> r = getCurrentAnswer();

        StemMultiColMap<Pair<List<String>, Double>> result = new StemMultiColMap<Pair<List<String>, Double>>();
        // list mapps to list to rating : X to Y to score

        for (List<String> k : knownExamples.keySet()) {
            result.put(k, knownExamples.get(k).getCountsSorted().get(0));
        }

        for (List<String> k : r.keySet()) {
            if (!result.containsKey(k)) {
                result.put(k, new Pair<List<String>, Double>(null, 0.0));
            }
        }

        return result;
    }

    /**
     * A wrapper for the original method, that returns only a single (the top)
     * answer for each key
     */
    public void transformFunctionalSyntactic()
            throws Exception {
        iterateSyntactic();// actually everything important happens in this method
    }



    public void printResult(
            StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImages) {
        for (List<String> k : keyToImages.keySet()) {
            System.out.println(k);

            StemMultiColMap<HashSet<MultiColMatchCase>> imageVals = keyToImages
                    .get(k);
            for (List<String> v : imageVals.keySet()) {
                System.out.println("--" + v);
                System.out.println("\t" + keyToImages.get(k).get(v));
            }
            System.out
                    .println("================================================");
        }
    }

    /**
     * @param t
     * @param anchorCols
     * @param depCols
     * @param knownExamples
     * @param closedWorld
     * @return Key -> ({good}, {bad})
     */
    public HashMap<List<String>, Pair<List<Double>, List<Double>>> checkKnownTupleMapping(
            Table t, int[] anchorCols, int[] depCols,
            StemMultiColMap<StemMultiColHistogram> knownExamples,
            boolean closedWorld) {
        HashMap<List<String>, Pair<List<Double>, List<Double>>> keyToGoodAndBad = new StemMultiColMap<Pair<List<Double>, List<Double>>>();
        if (!fuzzyMatching) {
            StemMultiColMap<StemMultiColHistogram> tableAsTree = createMapFromTable(
                    t, anchorCols, depCols);

            for (List<String> k : tableAsTree.keySet()) {
                if (knownExamples.containsKey(k)) {
                    List<Double> good = new ArrayList<Double>();
                    List<Double> bad = new ArrayList<Double>();

                    for (List<String> v : tableAsTree.get(k)
                            .getCountsUnsorted().keySet()) {
                        if (knownExamples.get(k).containsKey(v)) {
                            good.add(knownExamples.get(k).getScoreOf(v));
                        } else {
                            // bad.add(knownExamples.get(k).getCountsSorted().get(0).value);
                            bad.add(1.0);
                        }
                    }

                    keyToGoodAndBad.put(k,
                            new Pair<List<Double>, List<Double>>(good, bad));
                }

            }
        } else // With fuzzy matching
        {

            if (!(queryBuilder instanceof VerticaMultiColSimilarityQuerier)) {
                throw new IllegalStateException(
                        "VerticaSimilarityQuerier must be used with fuzzy matching.");
            }

            HashMap<List<String>, Histogram<List<String>>> tableAsTree = createExactMapFromTable(
                    t, anchorCols, depCols);

            HashMap<String, Collection<String>> valToSimilarValForms = ((VerticaMultiColSimilarityQuerier) queryBuilder).valToSimilarValForms;

            for (List<String> k : tableAsTree.keySet()) {
                List<Double> good = new ArrayList<Double>();
                List<Double> bad = new ArrayList<Double>();

                if (knownExamples.containsKey(k)) {

                    for (List<String> v : tableAsTree.get(k)
                            .getCountsUnsorted().keySet()) {
                        if (knownExamples.get(k).containsKey(v)) {
                            good.add(knownExamples.get(k).getScoreOf(v));
                        } else {

                            for (List<String> standardKey : exactOriginalExamples
                                    .keySet()) {
                                if (StemMultiColMap.stemList(standardKey)
                                        .equals(StemMultiColMap.stemList(k))) {
                                    List<String> standardValForm = null;

                                    for (List<String> standardV : exactOriginalExamples
                                            .get(standardKey)) {
                                        boolean allInRangeV = true;
                                        for (int i = 0; i < standardV.size(); i++) {
                                            if (!valToSimilarValForms.get(
                                                    standardV.get(i)).contains(
                                                    v.get(i))) {
                                                allInRangeV = false;
                                                break;
                                            }
                                        }
                                        if (allInRangeV) {
                                            standardValForm = standardV;
                                        }
                                    }

                                    // If not in range, penalize
                                    if (standardValForm != null) {
                                        good.add(Similarity.similarity(v,
                                                standardValForm));
                                    } else {
                                        // bad.add(knownExamples.get(k).getCountsSorted().get(0).value);
                                        bad.add(1.0);
                                    }
                                }
                            }
                        }
                    }

                    keyToGoodAndBad.put(k,
                            new Pair<List<Double>, List<Double>>(good, bad));
                } else // check if original and within distance
                {
                    for (List<String> standardKey : exactOriginalExamples
                            .keySet()) {
                        boolean allInRangeK = true;
                        for (int i = 0; i < standardKey.size(); i++) {
                            if (!valToSimilarValForms.get(standardKey.get(i))
                                    .contains(k.get(i))) {
                                allInRangeK = false;
                                break;
                            }
                        }
                        if (allInRangeK) {
                            for (List<String> v : tableAsTree.get(k)
                                    .getCountsUnsorted().keySet()) {
                                List<String> standardValForm = null;

                                for (List<String> standardV : exactOriginalExamples
                                        .get(standardKey)) {
                                    boolean allInRangeV = true;
                                    for (int i = 0; i < standardV.size(); i++) {
                                        if (!valToSimilarValForms.get(
                                                standardV.get(i)).contains(
                                                v.get(i))) {
                                            allInRangeV = false;
                                            break;
                                        }
                                    }
                                    if (allInRangeV) {
                                        standardValForm = standardV;
                                        break;
                                    }
                                }

                                // If not in range, penalize
                                if (standardValForm != null) {
                                    good.add(Similarity.similarity(v,
                                            standardValForm)
                                            * Similarity.similarity(k,
                                            standardKey));
                                } else {
                                    // bad.add(knownExamples.get(k).getCountsSorted().get(0).value);
                                    bad.add(Similarity.similarity(k,
                                            standardKey));
                                }
                            }
                            keyToGoodAndBad.put(k,
                                    new Pair<List<Double>, List<Double>>(good,
                                            bad));
                            break; // We pick the first key that matches
                        }
                    }

                }
            }
        }

        return keyToGoodAndBad;

    }

    /**
     * @param t
     * @param anchorCols
     * @param depCols
     * @param knownExamples
     * @param closedWorld
     * @return Key -> ({good}, {bad})
     */
    public HashMap<List<String>, Pair<Double, Double>> checkLocalCompleteness(
            Table t, int[] anchorCols, int[] depCols,
            StemMultiColMap<StemMultiColHistogram> knownExamples,
            boolean closedWorld) {
        HashMap<List<String>, Pair<Double, Double>> keyToPresentMissingValues = new HashMap<List<String>, Pair<Double, Double>>();

        if (!fuzzyMatching) {
            StemMultiColMap<StemMultiColHistogram> tableAsTree = createMapFromTable(
                    t, anchorCols, depCols);

            for (List<String> k : tableAsTree.keySet()) {
                if (knownExamples.containsKey(k)) {
                    double present = 0;
                    double absent = 0;

                    for (List<String> v : knownExamples.get(k)
                            .getCountsUnsorted().keySet()) {
                        // or weight?
                        // double w = 1.0;
                        double w = knownExamples.get(k).getScoreOf(v);

                        if (tableAsTree.get(k).containsKey(v)) {
                            present += w;
                        } else {
                            absent += w;
                        }
                    }

                    keyToPresentMissingValues.put(k, new Pair<Double, Double>(
                            present, absent));

                }
            }
        }

        return keyToPresentMissingValues;

    }

    /**
     * @param checkResult
     * @param localCompletenessCheck
     * @param closedWorld
     * @param weighted
     * @param localCompleteness
     * @return
     */
    public static boolean assessTupleMapping(
            HashMap<List<String>, Pair<List<Double>, List<Double>>> checkResult,
            HashMap<List<String>, Pair<Double, Double>> localCompletenessCheck,
            boolean closedWorld, boolean weighted, boolean localCompleteness) {

        HashMap<List<String>, Double> keyToCorrectnessForKey = new HashMap<>();
        HashMap<List<String>, Double> keyToWeight = new HashMap<>();
        double weightsSum = 0;
        int goodCount = 0;

        for (List<String> k : checkResult.keySet()) {
            double good = 0;
            double bad = 0;

            List<Double> goods = checkResult.get(k).key;
            List<Double> bads = checkResult.get(k).value;
            for (int i = 0; i < goods.size(); i++) {
                good += goods.get(i);
                goodCount += goods.size();
            }
            for (int i = 0; i < bads.size(); i++) {
                bad += bads.get(i);
            }

            double correctnessForK = (good * 1.0 / (good + bad));
            keyToCorrectnessForKey.put(k, correctnessForK);

            double weight;
            if (weighted) {
                // weight = goods.size() + bads.size();
                weight = good + bad;
            } else {
                weight = 1; // equal weights
            }

            keyToWeight.put(k, weight);
            weightsSum += weight;

        }

        double correctness = 0;

        for (List<String> k : keyToCorrectnessForKey.keySet()) {
            correctness += keyToCorrectnessForKey.get(k) * keyToWeight.get(k);
        }
        correctness /= weightsSum;

        // Average local completeness score for the table
        double presentSum = 0;
        double absentSum = 0;
        for (List<String> k : localCompletenessCheck.keySet()) {
            presentSum += localCompletenessCheck.get(k).key;
            absentSum += localCompletenessCheck.get(k).value;
        }
        double localCompletenessAverageScore = presentSum
                / (presentSum + absentSum);

        if (closedWorld) {
            return goodCount >= COVERAGE_THRESHOLD
                    && correctness >= QUALITY_THRESHOLD;
        } else if (localCompleteness) {
            return goodCount >= COVERAGE_THRESHOLD
                    && localCompletenessAverageScore >= QUALITY_THRESHOLD;
        } else {
            return goodCount >= COVERAGE_THRESHOLD;
        }
    }

    public static StemMultiColMap<StemMultiColHistogram> createMapFromTable(
            Table table, int[] colsFrom, int[] colsTo) {
        StemMultiColMap<StemMultiColHistogram> tableAsTree = new StemMultiColMap<StemMultiColHistogram>();

        for (Tuple tuple : table.getTuples()) {
            List<String> k = tuple.getValuesOfCells(colsFrom);
            if (!tableAsTree.containsKey(k)) {
                tableAsTree.put(k, new StemMultiColHistogram());
            }

            List<String> v = tuple.getValuesOfCells(colsTo);
            tableAsTree.get(k).increment(v);
        }

        return tableAsTree;
    }

    public static HashMap<List<String>, Histogram<List<String>>> createExactMapFromTable(
            Table table, int[] colsFrom, int[] colsTo) {
        HashMap<List<String>, Histogram<List<String>>> tableAsTree = new HashMap<>();

        for (Tuple tuple : table.getTuples()) {
            List<String> k = tuple.getValuesOfCells(colsFrom);
            if (!tableAsTree.containsKey(k)) {
                tableAsTree.put(k, new Histogram<List<String>>());
            }

            List<String> v = tuple.getValuesOfCells(colsTo);
            tableAsTree.get(k).increment(v);
        }

        return tableAsTree;
    }

    private static ArrayList<FunctionalDependency> findFDs(Table table)
            throws Exception {
        TANEjava tane = new TANEjava(table, FD_ERROR);
        tane.setStoplevel(2);
        return tane.getFD();
    }

    /**
     * Finds all FDs with ONLY anchorColIdx in LHS
     *
     * @param table
     * @param anchorColIdx
     * @return
     * @throws Exception
     */
    private static ArrayList<FunctionalDependency> findFDsWithColAsLHS(
            Table table, int anchorColIdx) throws Exception {
        ArrayList<FunctionalDependency> allFDs = findFDs(table);

        ArrayList<FunctionalDependency> fdsWithColInLHS = new ArrayList<>();

        for (FunctionalDependency fd : allFDs) {
            if (fd.getXIds().size() == 1 && fd.getXIds().contains(anchorColIdx)) {
                fdsWithColInLHS.add(fd);
            }
        }

        return fdsWithColInLHS;
    }

    /**
     * Finds all FDs with anchorColIdx in LHS
     *
     * @param table
     * @param anchorColIdx
     * @return
     * @throws Exception
     */
    private static ArrayList<FunctionalDependency> findFDsWithColInLHS(
            Table table, int anchorColIdx) throws Exception {
        ArrayList<FunctionalDependency> allFDs = findFDs(table);

        ArrayList<FunctionalDependency> fdsWithColInLHS = new ArrayList<>();

        for (FunctionalDependency fd : allFDs) {
            if (fd.getX().contains(anchorColIdx)) {
                fdsWithColInLHS.add(fd);
            }
        }

        return fdsWithColInLHS;
    }

    /**
     * Computes the score of a value given the evidence from the specified
     * tableIDs
     *
     * @param answers
     * @return probability distribution of the values (null = unknown...)
     */
    public StemMultiColHistogram computeAnswerScores(
            HashMap<List<String>, HashSet<MultiColMatchCase>> answers) {

        if (consolidation == ConsolidationMethod.FUNCTIONAL) {
            StemMultiColMap<Double> distrib = new StemMultiColMap<Double>();
            double scoreOfNone = 1.0;

            HashSet<MultiColMatchCase> allTables = new HashSet<MultiColMatchCase>();

            for (List<String> v : answers.keySet()) {
                distrib.put(v, 1.0);
                allTables.addAll(answers.get(v));
            }

            for (MultiColMatchCase MultiColMatchCase : allTables) {

                double tableRating = tableToRating.get(MultiColMatchCase);
                scoreOfNone *= (1 - tableRating);
                for (List<String> v : answers.keySet()) {
                    double s = distrib.get(v);

                    if (answers.get(v).contains(MultiColMatchCase)) {
                        s = s * tableRating;
                    } else {
                        s = s * (1 - tableRating);
                    }

                    distrib.put(v, s);
                }
            }

            // Normalize
            double sum = scoreOfNone;
            for (double s : distrib.values()) {
                sum += s;
            }

            StemMultiColHistogram normalizedDistrib = new StemMultiColHistogram();
            for (List<String> v : distrib.keySet()) {
                normalizedDistrib.increment(v, distrib.get(v) / sum);
            }

            return normalizedDistrib;
        } else if (consolidation == ConsolidationMethod.MAJORITY) {
            List<String> bestAnswer = null;
            int bestScore = -1;

            for (List<String> answer : answers.keySet()) {
                int score = answers.get(answer).size();
                if (score > bestScore) {
                    bestAnswer = answer;
                    bestScore = score;
                }
            }

            // Normalize
            StemMultiColHistogram normalizedDistrib = new StemMultiColHistogram();
            for (List<String> v : answers.keySet()) {
                if ((v == null && bestAnswer == null)
                        || (v != null && v.equals(bestAnswer))) {
                    normalizedDistrib
                            .increment(
                                    v,
                                    0.5 + (answers.get(bestAnswer).size() * 0.5 / tableToAnswers
                                            .size()));
                } else {
                    normalizedDistrib.increment(v, 0.0);
                }
            }

            return normalizedDistrib;
        } else if (consolidation == ConsolidationMethod.NON_FUNCTIONAL) {
            StemMultiColMap<Double> distrib = new StemMultiColMap<Double>();

            HashSet<MultiColMatchCase> allTables = new HashSet<MultiColMatchCase>();

            for (List<String> v : answers.keySet()) {
                distrib.put(v, 1.0);
                allTables.addAll(answers.get(v));
            }

            for (MultiColMatchCase MultiColMatchCase : allTables) {

                double tableRating = tableToRating.get(MultiColMatchCase);

                for (List<String> v : answers.keySet()) {
                    double s = distrib.get(v);

                    if (answers.get(v).contains(MultiColMatchCase)) {
                        s = s * (1 - tableRating);
                    }

                    distrib.put(v, s);
                }
            }

            StemMultiColHistogram normalizedDistrib = new StemMultiColHistogram();
            for (List<String> v : distrib.keySet()) {
                normalizedDistrib.increment(v, 1 - distrib.get(v));
            }

            return normalizedDistrib;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public class TableChecker implements Runnable {
        private StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImages;
        private MultiColTableLoader tableLoader;
        private MultiColMatchCase multiColMatchCase;

        public MultiColMatchCase getMultiColMatchCase() {
            return multiColMatchCase;
        }

        private boolean useNewlyDiscoveredKeys;
        private boolean closedWorld;
        /**
         * This tells whether examples should be weighted or of equal weight
         * when checking a table
         */
        private boolean useWeightedExamples;
        private boolean localCompleteness;

        public HashMap<MultiColMatchCase, Double> tableToPriorPartial = new HashMap<MultiColMatchCase, Double>();
        public HashMap<MultiColMatchCase, Double> tableToSchemaPriorPartial = new HashMap<MultiColMatchCase, Double>();
        public HashMap<MultiColMatchCase, HashSet<Pair<List<String>, List<String>>>> tableToAnswersPartial = new HashMap<MultiColMatchCase, HashSet<Pair<List<String>, List<String>>>>();
        public StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> result = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();

        public TableChecker(
                StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImages,
                MultiColTableLoader tableLoader,
                MultiColMatchCase multiColMatchCase,
                boolean useNewlyDiscoveredKeys, boolean closedWorld,
                boolean useWeightedExamples, boolean localCompleteness) {
            super();
            this.keyToImages = keyToImages;
            this.tableLoader = tableLoader;
            this.multiColMatchCase = multiColMatchCase;
            this.useNewlyDiscoveredKeys = useNewlyDiscoveredKeys;
            this.closedWorld = closedWorld;
            this.useWeightedExamples = useWeightedExamples;
            this.localCompleteness = localCompleteness;
        }

        @Override
        public void run() {
            try {
                int[] colsF = multiColMatchCase.col1;
                int[] colsT = multiColMatchCase.col2;

                Table webTable = tableLoader.loadTable(multiColMatchCase);

                HashMap<List<String>, Pair<List<Double>, List<Double>>> checkResult = checkKnownTupleMapping(
                        webTable, colsF, colsT, knownExamples, closedWorld);

                HashMap<List<String>, Pair<Double, Double>> localCompletenessCheck = checkLocalCompleteness(
                        webTable, colsF, colsT, knownExamples, closedWorld);

                if (assessTupleMapping(checkResult, localCompletenessCheck,
                        closedWorld, useWeightedExamples, localCompleteness)) {
                    // Transformation found

                    for (Tuple tup : webTable.getTuples()) {
                        List<String> k = tup.getValuesOfCells(colsF);
                        List<String> v = tup.getValuesOfCells(colsT); // the
                        // transformed
                        // value

                        k = StemMultiColMap.stemList(k);
                        v = StemMultiColMap.stemList(v);

                        if (k != null) {
                            if (!keyToImages.containsKey(k)
                                    && !useNewlyDiscoveredKeys) {
                                continue;
                            }

                            if (!result.containsKey(k)) {
                                result.put(
                                        k,
                                        new StemMultiColMap<HashSet<MultiColMatchCase>>());
                            }
                            if (!result.get(k).containsKey(v)) {
                                result.get(k).put(v,
                                        new HashSet<MultiColMatchCase>());
                            }

                            if (!tableToAnswersPartial
                                    .containsKey(multiColMatchCase)) {
                                tableToAnswersPartial
                                        .put(multiColMatchCase,
                                                new HashSet<Pair<List<String>, List<String>>>());
                            }
                            tableToAnswersPartial.get(multiColMatchCase).add(
                                    new Pair<List<String>, List<String>>(k, v));

                            result.get(k).get(v).add(multiColMatchCase);
                        }
                    }
                    tableToPriorPartial.put(multiColMatchCase, (double) webTable.confidence);

                    double schemaMatchScore;
                    if (webTable.hasHeader()) {
                        String[] h1 = webTable.getColumnMapping()
                                .getColumnNames(multiColMatchCase.col1);
                        String[] h2 = webTable.getColumnMapping()
                                .getColumnNames(multiColMatchCase.col2);

                        schemaMatchScore = computeSchemaMatch(h1, h2);
                    } else {
                        schemaMatchScore = schemaMatchPrior;
                    }
                    tableToSchemaPriorPartial.put(multiColMatchCase,
                            schemaMatchScore);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    public class TablePathChecker implements Runnable {
        private StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImages;
        private MultiColTableLoader tableLoader;
        private MultiColMatchCase multiColMatchCase;

        public MultiColMatchCase getMultiColMatchCase() {
            return multiColMatchCase;
        }

        private boolean useNewlyDiscoveredKeys;
        private boolean closedWorld;
        /**
         * This tells whether examples should be weighted or of equal weight
         * when checking a table
         */
        private boolean useWeightedExamples;
        private boolean localCompleteness;

        public HashMap<MultiColMatchCase, Double> tableToPriorPartial = new HashMap<MultiColMatchCase, Double>();
        public HashMap<MultiColMatchCase, Double> tableToSchemaPriorPartial = new HashMap<MultiColMatchCase, Double>();
        public HashMap<MultiColMatchCase, HashSet<Pair<List<String>, List<String>>>> tableToAnswersPartial = new HashMap<MultiColMatchCase, HashSet<Pair<List<String>, List<String>>>>();
        public StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> result = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();
        private HashMap<List<String>, Histogram<List<String>>> xzTree;
        private HashMap<List<String>, Histogram<List<String>>> zyTree;
        private Table zyTable;
        private Table xzTable;
        private int[] xColumns;

        public TablePathChecker(
                StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImages,
                Pair<Table, MultiColMatchCase> tablePair, Table xzTable,
                int[] x, HashMap<List<String>, Histogram<List<String>>> xzTree,
                boolean useNewlyDiscoveredKeys, boolean closedWorld,
                boolean useWeightedExamples, boolean localCompleteness) {

            super();
            this.keyToImages = keyToImages;
            this.xzTree = xzTree;
            this.xzTable = xzTable;
            this.xColumns = x;
            this.multiColMatchCase = tablePair.getValue();
            this.zyTable = tablePair.getKey();
            this.zyTree = createExactMapFromTable(tablePair.getKey(),
                    multiColMatchCase.col1, multiColMatchCase.col2);
            this.useNewlyDiscoveredKeys = useNewlyDiscoveredKeys;
            this.closedWorld = closedWorld;
            this.useWeightedExamples = useWeightedExamples;
            this.localCompleteness = localCompleteness;
        }

        @Override
        public void run() {
            try {

                for (List<String> x : xzTree.keySet()) {
                    List<String> z = xzTree.get(x).getCountsUnsorted().keySet()
                            .iterator().next();
                    if (z == null) {
                        continue;
                    }
                    if (!zyTree.containsKey(z)) {
                        continue;
                    }
                    List<String> y = zyTree.get(z).getCountsUnsorted().keySet()
                            .iterator().next();

                    if (x != null) {
                        if (!keyToImages.containsKey(x)
                                && !useNewlyDiscoveredKeys) {
                            continue;
                        }

                        if (!result.containsKey(x)) {
                            result.put(
                                    x,
                                    new StemMultiColMap<HashSet<MultiColMatchCase>>());
                        }
                        if (!result.get(x).containsKey(y)) {
                            result.get(x).put(y,
                                    new HashSet<MultiColMatchCase>());
                        }

                        if (!tableToAnswersPartial
                                .containsKey(multiColMatchCase)) {
                            tableToAnswersPartial
                                    .put(multiColMatchCase,
                                            new HashSet<Pair<List<String>, List<String>>>());
                        }
                        tableToAnswersPartial.get(multiColMatchCase).add(
                                new Pair<List<String>, List<String>>(x, y));

                        result.get(x).get(y).add(multiColMatchCase);
                    }
                }
                tableToPriorPartial.put(multiColMatchCase, zyTable.confidence * 0.5);// prior is half because of
                // indirection.

                double schemaMatchScore = schemaMatchPrior;
                if (zyTable.hasHeader()) {
                    String[] h2 = zyTable.getColumnMapping().getColumnNames(
                            multiColMatchCase.col2);
                    if (xzTable.hasHeader()) {
                        String[] h1 = xzTable.getColumnMapping()
                                .getColumnNames(xColumns);

                        schemaMatchScore = computeSchemaMatch(h1, h2);
                    }
                }
                tableToSchemaPriorPartial.put(multiColMatchCase,
                        schemaMatchScore);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    public void setGoodExample(List<String> k, List<String> v) {
        if (!knownExamples.containsKey(k)) {
            knownExamples.put(k, new StemMultiColHistogram());
        }
        knownExamples.get(k).setScoreOf(v, initialGoodExampleImportance);
        tableToAnswers.get(QUERY_TABLE_ID).add(
                new Pair<List<String>, List<String>>(k, v));

        if (!keyToImages.containsKey(k)) {
            keyToImages.put(k,
                    new StemMultiColMap<HashSet<MultiColMatchCase>>());
        }
        if (!keyToImages.get(k).containsKey(v)) {
            keyToImages.get(k).put(v, new HashSet<MultiColMatchCase>());
        }
        keyToImages.get(k).get(v).add(QUERY_TABLE_ID);

        if (!validatedExamples.containsKey(k)) {
            validatedExamples.put(k, new StemMultiColMap<Boolean>());
        }
        validatedExamples.get(k).put(v, true);

    }


    public void setBadExample(List<String> k, List<String> v) {
        if (!badExamples.containsKey(k)) {
            badExamples.put(k,
                    new StemMultiColMap<HashSet<MultiColMatchCase>>());
        }
        if (!badExamples.get(k).containsKey(v)) {
            if (keyToImages.containsKey(k) && keyToImages.get(k).containsKey(v)) {
                badExamples.get(k).put(v, keyToImages.get(k).get(v));
                keyToImages.get(k).remove(v);
            } else {
                badExamples.get(k).put(v, new HashSet<MultiColMatchCase>());
            }

        }

        if (knownExamples.containsKey(k) && knownExamples.get(k).containsKey(v)) {
            knownExamples.get(k).remove(v);
            if (knownExamples.get(k).getCountsUnsorted().size() == 0) {
                knownExamples.remove(k);
            }
        }

        if (!validatedExamples.containsKey(k)) {
            validatedExamples.put(k, new StemMultiColMap<Boolean>());
        }
        validatedExamples.get(k).put(v, true);

    }

    public void boostTables(StemMultiColMap<StemMultiColHistogram> result) {
        Histogram<String> tableToCount = new Histogram<>();
        int count = 0;
        for (List<String> k : result.keySet()) {
            for (List<String> v : result.get(k).getCountsUnsorted().keySet()) {
                count++;
                for (MultiColMatchCase MultiColMatchCase : keyToImages.get(k)
                        .get(v)) {
                    tableToCount.increment(MultiColMatchCase.tableID);
                }
            }
        }

        for (String tableID : tableToCount.getCountsUnsorted().keySet()) {
            double prior = VerticaBatchTableLoader.getPrior(tableID);
            VerticaBatchTableLoader.setPrior(tableID, prior + (1 - prior)
                    * tableToCount.getScoreOf(tableID) * SMOOTHING_FACTOR_ALPHA
                    / count);
        }

    }

    public StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> getProvenance() {
        return keyToImages;
    }

    /**
     * Best table that is not the input
     *
     * @param k
     * @param v
     * @return
     */
    public MultiColMatchCase getBestTableForAnswer(List<String> k,
                                                   List<String> v) {
        HashSet<MultiColMatchCase> tables = keyToImages.get(k).get(v);
        double bestRating = 0;
        MultiColMatchCase bestTable = null;
        for (MultiColMatchCase table : tables) {
            if (tableToRating.get(table) > bestRating) {
                bestTable = table;
                bestRating = tableToRating.get(table);
            }
        }

        return bestTable;
    }

    public boolean isNull(List<String> vals) {
        if (vals == null) {
            return true;
        } else {
            for (String v : vals) {
                if (v != null) {
                    return false;
                }
            }
            return true;
        }

    }

}
