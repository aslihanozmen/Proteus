package main;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

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

import main.transformer.precisionRecall.Functional;
import main.transformer.precisionRecall.FunctionalWithUserInteraction;
import model.*;
import model.columnMapping.AbstractColumnMatcher;
import model.multiColumn.MultiColTableLoader;

import model.multiColumn.VerticaMultiColBatchTableLoader;
import org.apache.commons.cli.*;
import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.lucene.queryparser.classic.ParseException;

import org.magicwerk.brownies.collections.GapList;
import query.multiColumn.MultiColTableQuerier;
import query.multiColumn.MultiColVerticaQuerier;
import query.multiColumn.VerticaMultiColSimilarityQuerier;
import test.GeneralTests;
import util.Histogram;
import util.MultiColMatchCase;
import util.Pair;
import util.Similarity;
import util.StemMultiColHistogram;
import util.StemMultiColMap;
import util.fd.tane.FunctionalDependency;
import util.fd.tane.TANEjava;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;
import static main.dataXFormerDriver.isUserInvolved;

/**
 * @author John Morcos
 * Supports set semantics, where each value is also composite
 */
public class WTTransformerMultiColSets {
    public static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
    public static int COVERAGE_THRESHOLD = 2;
    private static final double QUALITY_THRESHOLD = 0.0;
    public boolean fuzzyMatching = false;
    private static String inputCSVName = null;
    public static double schemaMatchPrior = 0.5;
    public static double schemaMatchWeight = 1;
    private List<MultiColMatchCase> tablesToCheckTransformer;

    public static boolean useOpenrank = false;

    public WTTransformerMultiColSets() {

    }

    public enum ConsolidationMethod {
        NON_FUNCTIONAL, FUNCTIONAL, MAJORITY
    }

    private ConsolidationMethod consolidation = ConsolidationMethod.FUNCTIONAL;

    public enum ValidationExamplesSelectionPolicy {
        XY_MIN_OVERLAP, X_MAX_NUM_TABLES, XY_MAX_NUM_TABLES, XY_MIN_NUM_TABLES, XY_MAX_COVER, XY_MIN_SCORE, CLUSTERING, TruthDiscovery, X_MAX_Y,
        /**
         * This one is for debugging/experiments purposes only. Assumes
         * GeneralTests.groundTruth is populated
         */
        ORACLE_BAD_EXAMPLES
    }

    private StemMultiColMap<StemMultiColMap<Boolean>> validatedExamples = new StemMultiColMap<>();
    public static final double BAD_EXAMPLE_WEIGHT = 10.0;
    public long runtime = 0;

    /**
     * Those 2 are for progress report
     */
    public String message = null;
    public boolean done = false;
    /*********************************/

    // private static final String ROOT_TABLE_CODE = "root";
    private static final double FD_ERROR = 0.10;

    private static final double SMOOTHING_FACTOR_ALPHA = 0.99;
    public static final MultiColMatchCase QUERY_TABLE_ID = new MultiColMatchCase("query",
            new int[]{0}, new int[]{1});
    public static final int MAX_ITERS_FOR_EM = 100;

    private ConcurrentHashMap<MultiColMatchCase, Double> tableToRating = new ConcurrentHashMap<MultiColMatchCase, Double>();
    private ConcurrentHashMap<MultiColMatchCase, Double> tableToPrior = new ConcurrentHashMap<MultiColMatchCase, Double>();
    private ConcurrentHashMap<MultiColMatchCase, Double> tableToSchemaScore = new ConcurrentHashMap<MultiColMatchCase, Double>();
    private List<MultiColMatchCase> allTablesToCheck = new GapList<>();
    private ConcurrentHashMap<MultiColMatchCase, HashSet<Pair<List<String>, List<String>>>> tableToAnswers = new ConcurrentHashMap<>();
    private Map<MultiColMatchCase, Double> bestScoredTables = new ConcurrentHashMap<>();
    private List<Table> allTablesSemantic = new GapList<>();

    private double initialGoodExampleImportance = 1;

    public static boolean originalOnly = false;
    //FIXME added term to idf Map
    private HashMap<String, Double> termToIdf = new HashMap<>();

    public static boolean verbose = true;

    private Table inputTable = null;
    public static TIntObjectHashMap<String> idToExternalId = new TIntObjectHashMap<>();
    public boolean toConvergence = true;
    private StemMultiColMap<StemMultiColHistogram> knownExamples = new StemMultiColMap<StemMultiColHistogram>();

    public StemMultiColMap<StemMultiColHistogram> getInitialExamplesFound() {
        return initialExamplesFound;
    }

    private StemMultiColMap<StemMultiColHistogram> initialExamplesFound = new StemMultiColMap<>();
    /**
     * These are the examples KNOWN to be bad (user-specified)
     */
    private StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> badExamples = new StemMultiColMap<>();

    private StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> goodExamples = new StemMultiColMap<>();

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
    private AbstractColumnMatcher[] colMatchersFrom;
    private AbstractColumnMatcher[] colMatchersTo;
    private boolean useNewlyDiscoveredKeys;
    private boolean augment;
    private HashSet<MultiColMatchCase> seenTriples = new HashSet<MultiColMatchCase>();
    private boolean noTablesLoaded = false;

    private VerticaMultiColSimilarityQuerier similarityQueryBuilder;

    public WTTransformerMultiColSets(Table inputTable, ConsolidationMethod consolidationMethod,
                                     int[] colsFrom, int[] colsTo, MultiColTableQuerier queryBuilder,
                                     final MultiColTableLoader tableLoader, final boolean closedWorld,
                                     final boolean useWeightedExamples, final boolean localCompleteness, int inductiveIters,
                                     final AbstractColumnMatcher[] colMatchersFrom,
                                     final AbstractColumnMatcher[] colMatchersTo, String[] keywords,
                                     final boolean useNewlyDiscoveredKeys, boolean augment, String inputCSVName) {
        this.inputCSVName = inputCSVName;
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
        this.colMatchersFrom = colMatchersFrom;
        this.colMatchersTo = colMatchersTo;
        this.useNewlyDiscoveredKeys = useNewlyDiscoveredKeys;
        this.augment = augment;

        for (Tuple t : inputTable.getTuples()) {
            List<String> k = t.getValuesOfCells(colsFrom);

            keyToImages.put(k, new StemMultiColMap<HashSet<MultiColMatchCase>>());
        }

        tableToAnswers.put(QUERY_TABLE_ID, new HashSet<Pair<List<String>, List<String>>>());

        for (Tuple tuple : inputTable.getTuples()) {
            if (!isNull(tuple.getValuesOfCells(colsTo))) {
                List<String> k = tuple.getValuesOfCells(colsFrom);
                List<String> v = tuple.getValuesOfCells(colsTo);

                if (!knownExamples.containsKey(k)) {
                    knownExamples.put(k, new StemMultiColHistogram());
                    initialExamplesFound.put(k, new StemMultiColHistogram());
                    validatedExamples.put(k, new StemMultiColMap<Boolean>());
                }

                knownExamples.get(k).increment(v, initialGoodExampleImportance);
                validatedExamples.get(k).put(v, true);

                exactOriginalExamples.put(k, new HashSet<List<String>>());

                HashSet<MultiColMatchCase> evidence = new HashSet<MultiColMatchCase>();
                evidence.add(QUERY_TABLE_ID);

                keyToImages.get(k).put(v, evidence);
                setGoodExample(k, v);
                exactOriginalExamples.get(k).add(v);
                tableToAnswers.get(QUERY_TABLE_ID).add(new Pair<List<String>, List<String>>(k, v));
            }
        }
    }

    public void setAllTablesSemantic(List<Table> allTablesSemantic) {
        this.allTablesSemantic = allTablesSemantic;
    }

    public Map<MultiColMatchCase, Double> getBestScoredTables() {
        return bestScoredTables;
    }

    public void setBestScoredTables(Map<MultiColMatchCase, Double> bestScoredTables) {
        this.bestScoredTables = bestScoredTables;
    }

    public long getRuntime() {
        return runtime;
    }

    public StemMultiColMap<StemMultiColHistogram> getKnownExamples() {
        return knownExamples;
    }

    public static void main(String[] args) throws Exception {


        int[] colsFrom = new int[]{0};
        int[] colsTo = new int[]{1};

        int inductiveIters = 1; // 1 means no induction

        /**
         * If true, it means that a value for an example different than what's
         * specified is deemed wrong
         */
        boolean closedWorld = false;
        boolean useWeightedExamples = true;
        boolean localCompleteness = true;
        AbstractColumnMatcher[] colMatchersFrom = null;
        AbstractColumnMatcher[] colMatchersTo = null;
        String[] keywords = null;
        boolean augment = false;
        try {
            Options clop = new Options();
            clop.addOption("qf", "query-file", true, "input (query) file");
            clop.addOption("ac", "anchor-columns", true, "anchor columns");
            clop.addOption("tc", "target-columns", true, "target columns");

            OptionGroup tblSrcGrp = new OptionGroup();
            tblSrcGrp.addOption(new Option("fs", "files", true,
                    "directory for a filesystem-based repository"));
            tblSrcGrp.addOption(new Option("db", "database", true,
                    "database name for database-based repository"));
            tblSrcGrp.addOption(new Option("dn", "dresden", true,
                    "directory containing Dresden .json files"));

            clop.addOptionGroup(tblSrcGrp);

            clop.addOption("ix", "index-dir", true, "index directory");

            clop.addOption("topk", true, "How many documents to fetch per lucene query");
            // clop.addOption("fd", true,
            // "Require functional transformation, with error threshold given");

            clop.addOption("i", "inductive", true,
                    "if present, makes inductive iterations, specified by the argument of this option");
            clop.addOption("cw", "closed-world", false, "specifies closed-world semantics");
            clop.addOption("lc", "local-completeness", false, "specifies local completeness");

            Option kwOpt = new Option("kw", "keywords", true,
                    "keywords to look for in the title and context of tables");
            kwOpt.setArgs(15);
            clop.addOption(kwOpt);

            Option mfOpt = new Option(
                    "mf",
                    "column-mapper-from",
                    true,
                    "choose column mapper for 'from' column ('st' for stemmer (default), 'kb' <absolute kbpath> for sparql-based, "
                            + "'fb' for Freebase (online), 'oc' for opencalais (cache file is in the classpath))");
            mfOpt.setArgs(2);
            clop.addOption(mfOpt);

            Option mtOpt = new Option(
                    "mt",
                    "column-mapper-to",
                    true,
                    "choose column mapper for 'to' column ('st' for stemmer (default), 'kb' <absolute kbpath> for sparql-based, "
                            + "'fb' for Freebase (online), 'oc' for opencalais (cache file is in the classpath))");
            mtOpt.setArgs(2);
            clop.addOption(mtOpt);

            clop.addOption("ag", "augment", false, "augments the table with newly found keys");

            CommandLineParser cliParse = new DefaultParser();

            CommandLine cl = cliParse.parse(clop, args);

            String inputTableFile = cl.getOptionValue("qf");

            String[] colsFromStr = cl.getOptionValue("ac").split(",");
            String[] colsToStr = cl.getOptionValue("tc").split(",");

            colsFrom = new int[colsFromStr.length];
            for (int i = 0; i < colsFromStr.length; i++) {
                colsFrom[i] = Integer.parseInt(colsFromStr[i]);
            }

            colsTo = new int[colsToStr.length];
            for (int i = 0; i < colsToStr.length; i++) {
                colsTo[i] = Integer.parseInt(colsToStr[i]);
            }

            MultiColTableLoader tableLoader = null;

            String indexDir = cl.getOptionValue("ix");

            if (cl.hasOption("i")) {
                inductiveIters = Integer.parseInt(cl.getOptionValue("i"));
            }

            if (cl.hasOption("cw")) {
                closedWorld = true;
            }
            if (cl.hasOption("we")) {
                useWeightedExamples = true;
            }
            if (cl.hasOption("ag")) {
                augment = true;
            }

            if (cl.hasOption("kw")) {
                keywords = cl.getOptionValues("kw");
            }

            Table inputTable = new Table(inputTableFile);

            // IndexReader indexReader =
            // DirectoryReader.open(FSDirectory.open(new File(indexDir)));

            String[] h1 = inputTable.getColumnMapping().getColumnNames(colsFrom);
            String[] h2 = inputTable.getColumnMapping().getColumnNames(colsTo);

            for (int i = 0; i < h1.length; i++) {
                if (h1[i] == null || h1[i].equalsIgnoreCase("COLUMN" + colsFrom[i])) {
                    h1[i] = null;
                }
            }
            for (int i = 0; i < h2.length; i++) {
                if (h2[i] == null || h2[i].equalsIgnoreCase("COLUMN" + colsTo[i])) {
                    h2[i] = null;
                }
            }

            MultiColVerticaQuerier queryBuilder = null;

            WTTransformerMultiColSets transformer = new WTTransformerMultiColSets(null,
                    ConsolidationMethod.FUNCTIONAL, colsFrom, colsTo, queryBuilder, tableLoader,
                    closedWorld, useWeightedExamples, localCompleteness, inductiveIters,
                    colMatchersFrom, colMatchersTo, keywords, augment, false, inputCSVName);

            transformer.iterate();
            transformer.printResult(transformer.getCurrentAnswer());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setInductiveIters(int indicutiveIters) {
        this.inductiveIters = indicutiveIters;
    }

    public void setQueryBuilder(MultiColTableQuerier queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public void setTableLoader(MultiColTableLoader tableLoader) {
        this.tableLoader = tableLoader;
    }

    /**
     * @return
     * @throws IOException
     * @throws ParseException
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws SQLException
     * @throws FileNotFoundException
     */
    public void iterate() throws Exception {


        long start = System.currentTimeMillis();
        long queryTime = 0;

        if (fuzzyMatching && !(queryBuilder instanceof VerticaMultiColSimilarityQuerier)) {
            throw new IllegalStateException("A VerticaSimilarityQuerier must be used"
                    + " when using fuzzy matching");
        }

        if (fuzzyMatching) {
            ((VerticaMultiColSimilarityQuerier) queryBuilder).exactKnownExamples = exactOriginalExamples;
        }
        noTablesLoaded = false;
        int iter = 0;
        StemMultiColMap<StemMultiColHistogram> lastKnownExamples = new StemMultiColMap<StemMultiColHistogram>();
        StemMultiColMap<String> foundXs = new StemMultiColMap<String>();
        //TODO Semantic Transformation Asli, just to make it faster for now
        boolean doQueries = iter < inductiveIters;

        done = false;
        message = "Initializing";

        answerToRating.putAll(knownExamples);
        HashSet<String> allTableIDs = new HashSet<String>();
        ArrayList<String> terms = new ArrayList<>();
        int timesLoadedIdf = 0;
        while (true) {
            terms.clear();
            // Clone knownExamples
            for (List<String> k : knownExamples.keySet()) {
                terms.addAll(k);
                StemMultiColHistogram s = new StemMultiColHistogram();
                for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                    terms.addAll(v);
                    s.increment(v, knownExamples.get(k).getScoreOf(v));
                }
                lastKnownExamples.put(k, s);
            }
            // -------------------
            terms.removeAll(Collections.singleton(null));
            ListIterator<String> iterator = terms.listIterator();
            while (iterator.hasNext()) {
                String s = iterator.next();
                s = s.replaceAll("'", "''");
                iterator.set(s.toLowerCase());
            }


            if (timesLoadedIdf == 0) {
                termToIdf = tableLoader.loadIdf(terms);
            }
            timesLoadedIdf++;

            int processedTablesCount = 0;
            StemMultiColMap<String> newlyFoundXs = new StemMultiColMap<String>();

            if (doQueries) {
                message = "Iteration " + (iter + 1) + " - querying for tables";
                //FIXME filter phase
                long start_query = System.currentTimeMillis();


                ArrayList<MultiColMatchCase> tablesToCheck = queryBuilder.findTables(knownExamples,
                        COVERAGE_THRESHOLD);
                GapList<String> tableIds = new GapList<>();
                for (MultiColMatchCase table : tablesToCheck) {
                    tableIds.add(table.tableID);
                }

                ((BatchTableLoader) tableLoader).loadTables(tableIds.toArray(new String[0]));

//                File dir = createDirectory("/3");
//                for (MultiColMatchCase multiColMatchCase : tablesToCheck) {
//                    Table webTable = tableLoader.loadTable(multiColMatchCase);
//                    File file = new File(dir.getAbsolutePath() + "/" + multiColMatchCase + ".csv");
//                    if(file.createNewFile()){
//                        continue;
//                    } else {
//                        PrintWriter writer = new PrintWriter(file);
//                        writer.print("");
//                        writer.close();
//                    }
//                    webTable.saveToCSVFile(file.getAbsolutePath());
//                }

                long benchmark = System.currentTimeMillis();
                System.out.println("runtime For Benchmark:" + (benchmark - start));
                tableIds.clear();


                System.out.println("**************TABLES TO CHECK*********** " + tablesToCheck);
                System.out.println("**************Number of tables to check*********** " + tablesToCheck.size());
                queryTime = System.currentTimeMillis() - start_query;
                System.out.println("Query Time was " + queryTime + " ms");


                if (tableLoader instanceof BatchTableLoader) {
                    HashSet<String> tableIDs = new HashSet<String>();
                    for (MultiColMatchCase multiColMatchCase : tablesToCheck) {
                        if (allTableIDs.contains(multiColMatchCase.tableID) || seenTriples.contains(multiColMatchCase)) {
                            continue;
                        }
                        tableIDs.add(multiColMatchCase.tableID);
                        allTableIDs.add(multiColMatchCase.tableID);
                        allTablesToCheck.add(multiColMatchCase);

                    }
                    if (tableIDs.isEmpty()) {
                        System.out.println("No new tables to load");
                        noTablesLoaded = true;
                        break;
                    }
                    long start_load = System.currentTimeMillis();
                    ((BatchTableLoader) tableLoader).loadTablesWithExamples(tableIDs.toArray(new String[0]), GeneralTests.exampleCountDir);
                    System.out.println("Table Loading was " + (System.currentTimeMillis() - start_load) + " ms");
                    queryTime += System.currentTimeMillis() - start_load;
                }

                long benchmarkAnalayzedTables = System.currentTimeMillis();
                System.out.println("runtime For Benchmark:" + (benchmarkAnalayzedTables - start));

                message = "Iteration " + (iter + 1) + " - checking tables";

                System.out.println("Found " + tablesToCheck.size() + " Table-Column-Combinations to Check!");
                ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREADS);
                ArrayList<TableChecker> tableCheckers = new ArrayList<WTTransformerMultiColSets.TableChecker>();
                for (final MultiColMatchCase MultiColMatchCase : tablesToCheck) {
                    if (seenTriples.contains(MultiColMatchCase)) {
                        continue;
                    } else {
                        if (originalOnly) {
                            Table webTable = tableLoader.loadTable(MultiColMatchCase);
                            MultiColMatchCase.setContext(new ArrayList<>(Arrays.asList(webTable.termSet)));
                            if (webTable.source != 1) {
                                continue;
                            }
                        }

                        seenTriples.add(MultiColMatchCase);
                        TableChecker tc = new TableChecker(keyToImages, tableLoader,
                                MultiColMatchCase, colMatchersFrom, colMatchersTo,
                                useNewlyDiscoveredKeys, closedWorld, useWeightedExamples,
                                localCompleteness);
                        tableCheckers.add(tc);
                        threadPool.submit(tc);
                    }

                }

                long endBenchmarkGulwani = System.currentTimeMillis();
                System.out.println("Runtime for Benchmark Gulwani: " + (endBenchmarkGulwani - start));

                threadPool.shutdown();
                while (true) {
                    try {
                        if (threadPool.awaitTermination(5, TimeUnit.HOURS)) {
                            break;
                        }
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }

                processedTablesCount += tableCheckers.size();

                message = "Iteration " + (iter + 1) + " - consolidating results";

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
                                keyToImages.put(k,
                                        new StemMultiColMap<HashSet<MultiColMatchCase>>());
                            }
                            for (List<String> v : partialResult.get(k).keySet()) {
                                if (badExamples.containsKey(k) && badExamples.get(k).containsKey(v)) {
                                    // add all its witness tables
                                    badExamples.get(k).get(v).addAll(partialResult.get(k).get(v));
                                } else {
                                    if (!keyToImages.get(k).containsKey(v)) {
                                        keyToImages.get(k).put(v, new HashSet<MultiColMatchCase>());
                                    }
                                    keyToImages.get(k).get(v).addAll(partialResult.get(k).get(v));
                                }

                            }

                            if (!foundXs.containsKey(k)) {
                                newlyFoundXs.put(k, "");
                                foundXs.put(k, "");
                            }
                        }
                    }
                }
            }//ENDIF QUERYIES

            // Maximization: revise table ratings
            tableToRating.put(QUERY_TABLE_ID, 1.0);
            HashSet<MultiColMatchCase> tablesToRemove = new HashSet<>();
            for (MultiColMatchCase MultiColMatchCase : tableToAnswers.keySet()) {

                if (MultiColMatchCase.equals(QUERY_TABLE_ID)) {
                    tableToRating.put(MultiColMatchCase, 1.0);
                    continue;
                }

                double rating = computeRating(MultiColMatchCase);
                if (rating > 0)
                    tableToRating.put(MultiColMatchCase, rating);
                else {
                    //System.out.println("removed tabled " + MultiColMatchCase);
                    tablesToRemove.add(MultiColMatchCase);
                }
            }

            //TODO remove bad tables
            removeDanglingAnswers(tablesToRemove);
            //System.out.println("*****Tables to remove **********: " + tablesToRemove);
            System.out.println("*****Amount of Tables to remove **********: " + tablesToRemove.size());
//            allTablesToCheck.removeAll(tablesToRemove);
//            File dir = createDirectory("/tablesToCheckAfterFiltering/3");
//            int index = 0;
//            for(MultiColMatchCase table : allTablesToCheck) {
//                createWebTableCsv(dir, table);
//                index++;
//                if(index == 33) {
//                    break;
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
                                    // try
                                    // {
                                    StemMultiColHistogram distrib = computeAnswerScores(keyToImages
                                            .get(k));
                                    return new Pair<List<String>, StemMultiColHistogram>(k, distrib);
                                }
                            }));
                }
            }

            es.shutdown();
            while (!es.awaitTermination(1, TimeUnit.HOURS)) {

            }

            for (Future<Pair<List<String>, StemMultiColHistogram>> future : futures) {
                List<String> k = future.get().key;
                StemMultiColHistogram distrib = future.get().value;
                double scoreOfNone = 1 - distrib.getTotalCount();
                Pair<List<String>, Double> bestEntry = distrib.getCountsSorted().get(0);
                double maxScore = bestEntry.value;
                if (consolidation == ConsolidationMethod.FUNCTIONAL) {
                    StemMultiColHistogram s = new StemMultiColHistogram();
                    s.increment(bestEntry.key, bestEntry.value);
                    knownExamples.put(k, s);
                    // //System.out.println(k + " -> " + bestEntry.key + " " +
                    // maxScore);
                } else {
                    knownExamples.put(k, distrib);
                }
                answerToRating.put(k, distrib);
            }

            int tid = 1;

            // Check for convergence
            if (!doQueries) {
                double errorSum = 0;

                for (List<String> k : knownExamples.keySet()) {
                    for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                        double s2 = knownExamples.get(k).getScoreOf(v);

                        double s1 = 0;
                        if (lastKnownExamples.containsKey(k)) {
                            if (lastKnownExamples.get(k).getCountsUnsorted().containsKey(v)) {
                                s1 = lastKnownExamples.get(k).getScoreOf(v);
                            }
                        }

                        errorSum += Math.abs(s2 - s1);
                    }
                }

                // For the other side
                for (List<String> k : lastKnownExamples.keySet()) {
                    for (List<String> v : lastKnownExamples.get(k).getCountsUnsorted().keySet()) {
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

                if (errorSum < 1e-5) {
                    break;
                }
            }


            iter++;

            if (iter >= MAX_ITERS_FOR_EM) {
                break;
            }

            if (iter >= inductiveIters || newlyFoundXs.size() == 0) {
                if (doQueries) // print only on the first iter we stop querying
                {
                    if (newlyFoundXs.size() == 0) {
                        //System.out.println("Can't cover more X's...no more queries");
                    }
                    if (iter >= inductiveIters) {
                        System.out.println("Reached the maximum allowed number of querying iterations");
                    }
                    System.out.println("#query_iterations: " + iter);
                }
                doQueries = false;

                message = "Can't cover more X's...no more queries to the corpus";
                if (!toConvergence) {
                    System.out.println("Convergence disabled. Ending now");
                    break;
                }
            }
        }
        System.out.println("#iterations: " + iter);
        done = true;
        long end = System.currentTimeMillis();
        runtime = (end - start) - queryTime;
        System.out.println("RUNTIME: " + runtime);

        pruneBadTablesAndAnswers();


//        Table[] tablesArray;
//        System.out.println("Best Rated Tables: " + tableToRating.size());
//        System.out.println("Best Rated Tables: " + tableToRating);
//        File dir = createDirectory("/bestRatedTables/3");
//        if(tableToRating.size() < 33) {
//
//                for(Map.Entry<MultiColMatchCase, Double> entry : tableToRating.entrySet()) {
//                    MultiColMatchCase table = entry.getKey();
//                    if(!entry.getValue().equals(0.0) && !table.getTableID().equals("query")) {
//                        //Table webTable = createWebTableCsv(dir, table);
//                        //allTablesSemantic.add(webTable);
//                    }
//                }
//            } else {
//                Map<MultiColMatchCase, Double> sortedTableToRating = tableToRating
//                        .entrySet()
//                        .stream()
//                        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
//                        .collect(
//                                toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
//                                        LinkedHashMap::new));
//
//                //tablesArray = new Table[32];
//                int index = 0;
//                for(Map.Entry<MultiColMatchCase, Double> entry : sortedTableToRating.entrySet()) {
//                    MultiColMatchCase table = entry.getKey();
//                    if(!entry.getValue().equals(0.0) && !table.getTableID().equals("query")) {
//                        //Table webTable = createWebTableCsv(dir, table);
//                        //tablesArray[index] =  webTable;
//                        index++;
//                        if(index == 32) {
//                            break;
//                        }
//                    }
//                }
//            }

    }


    public void iterateSyntactic() throws Exception {


        long start = System.currentTimeMillis();
        long queryTime = 0;

        if (fuzzyMatching && !(queryBuilder instanceof VerticaMultiColSimilarityQuerier)) {
            throw new IllegalStateException("A VerticaSimilarityQuerier must be used"
                    + " when using fuzzy matching");
        }

        if (fuzzyMatching) {
            ((VerticaMultiColSimilarityQuerier) queryBuilder).exactKnownExamples = exactOriginalExamples;
        }
        noTablesLoaded = false;
        int iter = 0;
        StemMultiColMap<StemMultiColHistogram> lastKnownExamples = new StemMultiColMap<StemMultiColHistogram>();
        StemMultiColMap<String> foundXs = new StemMultiColMap<String>();
        boolean doQueries = iter < inductiveIters;

        done = false;
        message = "Initializing";

        answerToRating.putAll(knownExamples);
        HashSet<String> allTableIDs = new HashSet<String>();
        ArrayList<String> terms = new ArrayList<>();
        int timesLoadedIdf = 0;
        while (true) {
            terms.clear();
            // Clone knownExamples
            for (List<String> k : knownExamples.keySet()) {
                terms.addAll(k);
                StemMultiColHistogram s = new StemMultiColHistogram();
                for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                    terms.addAll(v);
                    s.increment(v, knownExamples.get(k).getScoreOf(v));
                }
                lastKnownExamples.put(k, s);
            }
            // -------------------
            terms.removeAll(Collections.singleton(null));
            ListIterator<String> iterator = terms.listIterator();
            while (iterator.hasNext()) {
                String s = iterator.next();
                s = s.replaceAll("'", "''");
                iterator.set(s.toLowerCase());
            }


            if (timesLoadedIdf == 0) {
                termToIdf = tableLoader.loadIdf(terms);
            }
            timesLoadedIdf++;

            int processedTablesCount = 0;
            StemMultiColMap<String> newlyFoundXs = new StemMultiColMap<String>();

            if (doQueries) {
                message = "Iteration " + (iter + 1) + " - querying for tables";
                //FIXME filter phase
                long start_query = System.currentTimeMillis();


                ArrayList<MultiColMatchCase> tablesToCheck = queryBuilder.findTables(knownExamples,
                        COVERAGE_THRESHOLD);
                GapList<String> tableIds = new GapList<>();
                for (MultiColMatchCase table : tablesToCheck) {
                    tableIds.add(table.tableID);
                }

                ((BatchTableLoader) tableLoader).loadTables(tableIds.toArray(new String[0]));
                tablesToCheckTransformer = new GapList<>(tablesToCheck);

                long benchmark = System.currentTimeMillis();
                System.out.println("runtime For Benchmark:" + (benchmark - start));
                tableIds.clear();


                System.out.println("**************TABLES TO CHECK*********** " + tablesToCheck);
                System.out.println("**************Number of tables to check*********** " + tablesToCheck.size());
                queryTime = System.currentTimeMillis() - start_query;
                System.out.println("Query Time was " + queryTime + " ms");


                if (tableLoader instanceof BatchTableLoader) {
                    HashSet<String> tableIDs = new HashSet<String>();
                    for (MultiColMatchCase multiColMatchCase : tablesToCheck) {
                        if (allTableIDs.contains(multiColMatchCase.tableID) || seenTriples.contains(multiColMatchCase)) {
                            continue;
                        }
                        tableIDs.add(multiColMatchCase.tableID);
                        allTableIDs.add(multiColMatchCase.tableID);
                        allTablesToCheck.add(multiColMatchCase);

                    }
                    if (tableIDs.isEmpty()) {
                        System.out.println("No new tables to load");
                        noTablesLoaded = true;
                        break;
                    }
                    long start_load = System.currentTimeMillis();
                    ((BatchTableLoader) tableLoader).loadTablesWithExamples(tableIDs.toArray(new String[0]), GeneralTests.exampleCountDir);
                    System.out.println("Table Loading was " + (System.currentTimeMillis() - start_load) + " ms");
                    queryTime += System.currentTimeMillis() - start_load;
                }


                // getAnalyzedTables((VerticaMultiColBatchTableLoader) tableLoader);
                long benchmarkAnalayzedTables = System.currentTimeMillis();
                System.out.println("runtime For Benchmark:" + (benchmarkAnalayzedTables - start));

                message = "Iteration " + (iter + 1) + " - checking tables";

                System.out.println("Found " + tablesToCheck.size() + " Table-Column-Combinations to Check!");
                ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREADS);
                ArrayList<TableChecker> tableCheckers = new ArrayList<WTTransformerMultiColSets.TableChecker>();
                for (final MultiColMatchCase MultiColMatchCase : tablesToCheck) {
                    if (seenTriples.contains(MultiColMatchCase)) {
                        continue;
                    } else {
                        if (originalOnly) {
                            Table webTable = tableLoader.loadTable(MultiColMatchCase);
                            MultiColMatchCase.setContext(new ArrayList<>(Arrays.asList(webTable.termSet)));
                            if (webTable.source != 1) {
                                continue;
                            }
                        }

                        seenTriples.add(MultiColMatchCase);
                        TableChecker tc = new TableChecker(keyToImages, tableLoader,
                                MultiColMatchCase, colMatchersFrom, colMatchersTo,
                                useNewlyDiscoveredKeys, closedWorld, useWeightedExamples,
                                localCompleteness);
                        tableCheckers.add(tc);
                        threadPool.submit(tc);
                    }

                }

                long endBenchmarkGulwani = System.currentTimeMillis();
                System.out.println("Runtime for Benchmark Gulwani: " + (endBenchmarkGulwani - start));

                threadPool.shutdown();
                while (true) {
                    try {
                        if (threadPool.awaitTermination(5, TimeUnit.HOURS)) {
                            break;
                        }
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }

                processedTablesCount += tableCheckers.size();

                message = "Iteration " + (iter + 1) + " - consolidating results";

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
                                keyToImages.put(k,
                                        new StemMultiColMap<HashSet<MultiColMatchCase>>());
                            }
                            for (List<String> v : partialResult.get(k).keySet()) {
                                if (badExamples.containsKey(k) && badExamples.get(k).containsKey(v)) {
                                    // add all its witness tables
                                    badExamples.get(k).get(v).addAll(partialResult.get(k).get(v));
                                } else {
                                    if (!keyToImages.get(k).containsKey(v)) {
                                        keyToImages.get(k).put(v, new HashSet<MultiColMatchCase>());
                                    }
                                    keyToImages.get(k).get(v).addAll(partialResult.get(k).get(v));
                                }

                            }

                            if (!foundXs.containsKey(k)) {
                                newlyFoundXs.put(k, "");
                                foundXs.put(k, "");
                            }
                        }
                    }
                }
            }//ENDIF QUERYIES

            // Maximization: revise table ratings
            tableToRating.put(QUERY_TABLE_ID, 1.0);
            HashSet<MultiColMatchCase> tablesToRemove = new HashSet<>();
            for (MultiColMatchCase MultiColMatchCase : tableToAnswers.keySet()) {

                if (MultiColMatchCase.equals(QUERY_TABLE_ID)) {
                    tableToRating.put(MultiColMatchCase, 1.0);
                    continue;
                }

                double rating = computeRating(MultiColMatchCase);
                if (rating > 0)
                    tableToRating.put(MultiColMatchCase, rating);
                else {
                    //System.out.println("removed tabled " + MultiColMatchCase);
                    tablesToRemove.add(MultiColMatchCase);
                }
            }

            //TODO remove bad tables
            removeDanglingAnswers(tablesToRemove);
            System.out.println("*****Amount of Tables to remove **********: " + tablesToRemove.size());


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
                                    // try
                                    // {
                                    StemMultiColHistogram distrib = computeAnswerScores(keyToImages
                                            .get(k));
                                    return new Pair<List<String>, StemMultiColHistogram>(k, distrib);
                                }
                            }));
                }
            }

            es.shutdown();
            while (!es.awaitTermination(1, TimeUnit.HOURS)) {

            }

            for (Future<Pair<List<String>, StemMultiColHistogram>> future : futures) {
                List<String> k = future.get().key;
                StemMultiColHistogram distrib = future.get().value;
                double scoreOfNone = 1 - distrib.getTotalCount();
                Pair<List<String>, Double> bestEntry = distrib.getCountsSorted().get(0);
                double maxScore = bestEntry.value;
                if (consolidation == ConsolidationMethod.FUNCTIONAL) {
                    StemMultiColHistogram s = new StemMultiColHistogram();
                    s.increment(bestEntry.key, bestEntry.value);
                    knownExamples.put(k, s);
                    // //System.out.println(k + " -> " + bestEntry.key + " " +
                    // maxScore);
                } else {
                    knownExamples.put(k, distrib);
                }
                answerToRating.put(k, distrib);
            }

            int tid = 1;

            // Check for convergence
            if (!doQueries) {
                double errorSum = 0;

                for (List<String> k : knownExamples.keySet()) {
                    for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                        double s2 = knownExamples.get(k).getScoreOf(v);

                        double s1 = 0;
                        if (lastKnownExamples.containsKey(k)) {
                            if (lastKnownExamples.get(k).getCountsUnsorted().containsKey(v)) {
                                s1 = lastKnownExamples.get(k).getScoreOf(v);
                            }
                        }

                        errorSum += Math.abs(s2 - s1);
                    }
                }

                // For the other side
                for (List<String> k : lastKnownExamples.keySet()) {
                    for (List<String> v : lastKnownExamples.get(k).getCountsUnsorted().keySet()) {
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

                if (errorSum < 1e-5) {
                    break;
                }
            }


            iter++;

            if (iter >= MAX_ITERS_FOR_EM) {
                break;
            }

            if (iter >= inductiveIters || newlyFoundXs.size() == 0) {
                if (doQueries) // print only on the first iter we stop querying
                {
                    if (newlyFoundXs.size() == 0) {
                        //System.out.println("Can't cover more X's...no more queries");
                    }
                    if (iter >= inductiveIters) {
                        System.out.println("Reached the maximum allowed number of querying iterations");
                    }
                    System.out.println("#query_iterations: " + iter);
                }
                doQueries = false;

                message = "Can't cover more X's...no more queries to the corpus";
                if (!toConvergence) {
                    System.out.println("Convergence disabled. Ending now");
                    break;
                }
            }
        }
        System.out.println("#iterations: " + iter);
        done = true;
        long end = System.currentTimeMillis();
        runtime = (end - start) - queryTime;
        System.out.println("RUNTIME: " + runtime);

        pruneBadTablesAndAnswers();


        Table[] tablesArray;
        System.out.println("Best Rated Tables: " + tableToRating.size());
        System.out.println("Best Rated Tables: " + tableToRating);
        File dir = createDirectory("/bestRatedTables");
        if (tableToRating.size() == 1) {
            for (MultiColMatchCase multiColMatchCase : tablesToCheckTransformer) {
                createWebTableCsv(dir, multiColMatchCase);
            }
        } else if (tableToRating.size() < 33) {
            for (Map.Entry<MultiColMatchCase, Double> entry : tableToRating.entrySet()) {
                MultiColMatchCase table = entry.getKey();
                if (!entry.getValue().equals(0.0) && !table.getTableID().equals("query")) {
                    Table webTable = createWebTableCsv(dir, table);
                    allTablesSemantic.add(webTable);
                }
            }
        } else {
            Map<MultiColMatchCase, Double> sortedTableToRating = tableToRating
                    .entrySet()
                    .stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .collect(
                            toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                    LinkedHashMap::new));

            tablesArray = new Table[32];
            int index = 0;
            for (Map.Entry<MultiColMatchCase, Double> entry : sortedTableToRating.entrySet()) {
                MultiColMatchCase table = entry.getKey();
                if (!entry.getValue().equals(0.0) && !table.getTableID().equals("query")) {
                    Table webTable = createWebTableCsv(dir, table);
                    tablesArray[index] = webTable;
                    index++;
                    if (index == 32) {
                        break;
                    }
                }
            }
        }
        if(!isUserInvolved){
            Functional.precisionRecall("./experiment/tablesToCheck",
                    false);
        } else {
            FunctionalWithUserInteraction.precisionRecall("./experiment/tablesToCheck",
                    false);
        }
    }


    public Table createWebTableCsv(File dir, MultiColMatchCase table) throws SQLException, IOException {
        Table webTable = tableLoader.loadTable(table);
        File file = new File(dir.getAbsolutePath() + "/" + table + ".csv");
        if (!file.createNewFile()) {
            PrintWriter writer = new PrintWriter(file);
            writer.print("");
            writer.close();
        }
        webTable.saveToCSVFile(file.getAbsolutePath());
        return webTable;
    }

    public Table createWebTableCsvWithTable(File dir, Table table, String tableID) throws IOException {

        File file = new File(dir.getAbsolutePath() + "/" + tableID + ".csv");
        if (file.createNewFile()) {
            System.out.println("Recreating the file");
        } else {
            PrintWriter writer = new PrintWriter(file);
            writer.print("");
            writer.close();
        }
        table.saveToCSVFile(file.getAbsolutePath());
        return table;
    }

    public File createDirectory(String s3) {
        File dir = new File("./experiment/tablesToCheck/" + new File(getInputCSVName()).getName() + s3);
        if (!dir.exists()) {
            if (dir.mkdirs()) {
                System.out.println("Directory for benchmark is created");
            }
        }
        return dir;
    }

    public void getAnalyzedTables(VerticaMultiColBatchTableLoader tablesToCheck) throws IOException, SQLException {
        Similarity.setThreadshold(23);
        Map<String, String> searchedValues = new HashMap<>();
        Map<String[], List<Table>> bestTablesWithExamples = new HashMap<>();
        String input;
        String output;
        if (consolidation == ConsolidationMethod.FUNCTIONAL) {
            for (Map.Entry<List<String>, StemMultiColHistogram> example : knownExamples.entrySet()) {
                input = example.getKey().get(0);
                output = example.getValue().getCountsUnsorted().keySet().iterator().next().get(0);
                searchedValues.put(input, output);
                if (input != null && output != null) {
                    String[] examplePair = new String[]{input.toLowerCase(), output.toLowerCase()};
                    bestTablesWithExamples.put(examplePair, new GapList<>());
                }
            }
        }

//        else if(consolidation == ConsolidationMethod.NON_FUNCTIONAL){
//            for(Map.Entry<List<String>, StemMultiColHistogram> example : knownExamples.entrySet()) {
//                input = example.getKey().get(0);
//                output = example.getValue().getCountsUnsorted().keySet().iterator().next().get(0);
//                searchedValues.put(input,output);
//                String[] examplePair = new String[] {input.toLowerCase(), output.toLowerCase()};
//                bestTablesWithExamples.put(examplePair, new GapList<>());
//            }
//        }

        List<Table> bestRatedTables = new GapList<>();
        int tableId = 0;
        for (Table table : tablesToCheck.getBuffer().values()) {
            tableId++;
            table.setTableName(String.valueOf(tableId));
            Set<String> valueableInputStrings = new HashSet<>();
            Set<String> valueableOutputStrings = new HashSet<>();
            checkIfTableHasAtLeastTwoSimilarPairExample(searchedValues, bestRatedTables,
                    table, valueableInputStrings, valueableOutputStrings, bestTablesWithExamples);
        }

        for (String[] examplePair : bestTablesWithExamples.keySet()) {
            File dir = createDirectory("/bestRatedTables");
            List<Table> tables = bestTablesWithExamples.get(examplePair).stream()
                    .collect(collectingAndThen(toCollection(() -> new TreeSet<>(Comparator.comparing(Table::getTableName))),
                            ArrayList::new));
            for (Table table : tables) {
                createWebTableCsvWithTable(dir, table, table.getTableName());
            }
        }

        Similarity.setThreadshold(3);
        System.out.println("number of analyzed tables: " + bestRatedTables.stream()
                .collect(collectingAndThen(toCollection(() -> new TreeSet<>(Comparator.comparing(Table::getTableName))),
                        ArrayList::new)).size());
    }

    public void checkIfTableHasAtLeastTwoSimilarPairExample(Map<String, String> searchedValues, List<Table> bestRatedTables,
                                                            Table table, Set<String> valueableInputStrings,
                                                            Set<String> valueableOutputStrings, Map<String[], List<Table>> bestTablesWithExamples) {
        boolean hasInput = false;
        boolean hasOutput = false;
        for (Map.Entry<String, String> example : searchedValues.entrySet()) {
            if (example.getKey() != null && example.getValue() != null) {
                String inputLowerCase = example.getKey().toLowerCase();
                String outputLowerCase = example.getValue().toLowerCase();
                String[] examplePair = new String[]{inputLowerCase, outputLowerCase};
                int howManySubstringMatch = 0;
                for (Tuple tuple : table.getTuples()) {
                    hasInput = false;
                    hasOutput = false;
                    for (Cell cell : tuple.getCells()) {
                        if (cell != null && cell.getValue() != null && (cell.getValue().length() > 1)) {
                            String cellLowerCase = cell.getValue().toLowerCase();
                            hasInput = isHasPair(valueableInputStrings, hasInput, inputLowerCase, cell, cellLowerCase);
                            hasOutput = isHasPair(valueableOutputStrings, hasOutput, outputLowerCase, cell, cellLowerCase);
                        }
                        if (isTupleHadSimilarExample(hasInput && hasOutput)) break;
                    }
                    if (hasInput && hasOutput) {
                        howManySubstringMatch++;
                        if (howManySubstringMatch >= 1) {
                            break;
                        }
                    }
                }
                checkIfExamplesAlignedInSameCols(bestRatedTables, table, howManySubstringMatch, valueableInputStrings,
                        valueableOutputStrings, hasInput, hasOutput, examplePair, bestTablesWithExamples);
            }
        }
    }

    public boolean isHasPair(Set<String> valueableStrings,
                             boolean hasPair, String pairLowerCase,
                             Cell cell, String cellLowerCase) {
        if (!hasPair) {
            if ((cellLowerCase.contains(pairLowerCase) || pairLowerCase.contains(cellLowerCase))
                    && (cell.isSimilarValue(pairLowerCase))) {
                hasPair = true;
                valueableStrings.add(cell.getValue());
            }
        }
        return hasPair;
    }

    public boolean isTupleHadSimilarExample(boolean b) {
        return b;
    }

    public void checkIfExamplesAlignedInSameCols(List<Table> bestRatedTables, Table table, int index,
                                                 Set<String> valueableInputStrings, Set<String> valueableOutputStrings,
                                                 boolean hasInput, boolean hasOutput, String[] examplePair,
                                                 Map<String[], List<Table>> bestTablesWithExamples) {
        // TODO Asli Index
        if (hasInput && hasOutput && index >= 1) {
            boolean hasAllInputValuesInOneColumn = false;
            boolean hasAllOutputValuesInOneColumn = false;
            for (int i = 0; i < table.getNumCols(); i++) {
                Set<String> colVals = table.getColumnValues(i);
                if (colVals.containsAll(valueableInputStrings)) {
                    hasAllInputValuesInOneColumn = true;
                } else if (colVals.containsAll(valueableOutputStrings)) {
                    hasAllOutputValuesInOneColumn = true;
                }
                if (hasAllInputValuesInOneColumn && hasAllOutputValuesInOneColumn) {
                    for (String[] originalPair : bestTablesWithExamples.keySet()) {
                        if (originalPair[0].equalsIgnoreCase(examplePair[0])) {
                            bestTablesWithExamples.get(originalPair).add(table);
                        }
                    }
                    bestRatedTables.add(table);
                    break;
                }
            }

        }
    }

    public void runEM() throws Exception {
        int oldInductiveIters = inductiveIters;
        boolean oldToConvergence = toConvergence;

        this.setInductiveIters(0); // No querying
        this.toConvergence = true;

        iterate();

        this.setInductiveIters(oldInductiveIters);
        this.toConvergence = oldToConvergence;
    }

    /**
     * Add another version that removes answers which co-occur ONLY with bad
     * examples
     */
    public void pruneBadTablesAndAnswers() {
        // Go through tables, remove those below the quality threshold
        HashSet<MultiColMatchCase> tablesToRemove = new HashSet<MultiColMatchCase>();
        for (MultiColMatchCase table : tableToRating.keySet()) {
            if (table.getTableID().equals("query"))
                continue;
            double oldrating = tableToRating.get(table);
            double newrating = recomputeRating(table);
            if (newrating < 0) {
                tablesToRemove.add(table);
            } else {
                tableToRating.put(table, newrating);
            }
        }

        // Remove answers with no remaining tables


        removeDanglingAnswers(tablesToRemove);
    }

    private void removeDanglingAnswers(HashSet<MultiColMatchCase> tablesToRemove) {
        HashSet<Pair<List<String>, List<String>>> answersToRemove = new HashSet<Pair<List<String>, List<String>>>();
        for (MultiColMatchCase table : tablesToRemove) {
            for (Pair<List<String>, List<String>> answer : tableToAnswers.get(table)) {
                // If marked as bad, it's not in keyToImages...so skip
                if (badExamples.containsKey(answer.key)
                        && badExamples.get(answer.key).containsKey(answer.value)) {
                    continue;
                }
                boolean allTablesAreBad = true;
                for (MultiColMatchCase supportingTable : keyToImages.get(answer.key).get(
                        answer.value)) {
                    if (!tablesToRemove.contains(supportingTable)) {
                        allTablesAreBad = false;
                        break;
                    }
                }
                if (allTablesAreBad) {
                    answersToRemove.add(answer);
                } else {
                    keyToImages.get(answer.key).get(answer.value).remove(table);
                }
            }
        }

        for (Pair<List<String>, List<String>> answer : answersToRemove) {
            // Remove this answer from the system
            for (MultiColMatchCase supportingTable : keyToImages.get(answer.key).get(answer.value)) {
                tableToAnswers.get(supportingTable).remove(answer);
            }
            answerToRating.remove(answer);
            // keyToImages.get(answer.key).remove(answer.value);
            // knownExamples.get(answer.key).remove(answer.value);
            // TODO: or:
            setBadExample(answer.key, answer.value);
            // TODO 2: keep it for potential future use?
        }

        for (MultiColMatchCase table : tablesToRemove) {
            tableToRating.remove(table);
            tableToAnswers.remove(table);
        }
    }

    public String getInputCSVName() {
        return inputCSVName;
    }

    /**
     * TODO: Add another version that removes answers which co-occur ONLY with
     * bad examples
     */
    public void removeAnswersWithOnlyBadAnswers() {
        HashSet<Pair<List<String>, List<String>>> answersToRemove = new HashSet<Pair<List<String>, List<String>>>();

        for (List<String> k : keyToImages.keySet()) {
            for (List<String> v : keyToImages.get(k).keySet()) {
                Pair<List<String>, List<String>> answer = new Pair<List<String>, List<String>>(k, v);

                boolean alwaysWithABadExample = true;
                for (MultiColMatchCase table : keyToImages.get(k).get(v)) {
                    // if not a bad table, i.e., no bad examples in this table
                    boolean isAGoodTable = true;
                    int goodExamplesCount = 0;
                    int badExamplesCount = 0;
                    for (Pair<List<String>, List<String>> a : tableToAnswers.get(table)) {
                        if (badExamples.containsKey(a.key)
                                && badExamples.get(a.key).containsKey(a.value)) {
                            ++badExamplesCount;
                            if (badExamplesCount > 3) {
                                break;
                            }
                        }
                    }


                    if (badExamplesCount <= 3) {
                        alwaysWithABadExample = false;
                        break;
                    }
                }

                if (alwaysWithABadExample) {
                    answersToRemove.add(answer);
                }

            }
        }

        System.out.println(answersToRemove.size() + " potential answers to remove");
        for (Pair<List<String>, List<String>> answer : answersToRemove) {
            // Remove this answer from the system

            for (MultiColMatchCase supportingTable : keyToImages.get(answer.key).get(answer.value)) {
                tableToAnswers.get(supportingTable).remove(answer);
            }

            answerToRating.remove(answer);

            // keyToImages.get(answer.key).remove(answer.value);
            // knownExamples.get(answer.key).remove(answer.value);
            // TODO: or:
            setBadExample(answer.key, answer.value);

            // TODO 2: keep it for potential future use?
        }
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
                keyToImagesFinal.put(k, new StemMultiColMap<HashSet<MultiColMatchCase>>());

                if (knownExamples.containsKey(k)) {
                    if (consolidation == ConsolidationMethod.FUNCTIONAL
                            || consolidation == ConsolidationMethod.MAJORITY) {
                        if (knownExamples.get(k).getCountsUnsorted().size() > 0) {
                            List<String> v = knownExamples.get(k).getCountsSorted().get(0).key;
                            keyToImagesFinal.get(k).put(v, keyToImages.get(k).get(v));
                        }
                    } else if (consolidation == ConsolidationMethod.NON_FUNCTIONAL) {
                        for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                            keyToImagesFinal.get(k).put(v, keyToImages.get(k).get(v));
                        }
                    }
                }

            }
            return keyToImagesFinal;
        }
    }

    /**
     * Selects the most informative examples for labeling
     *
     * @param nExamples
     * @return
     */
    public List<Pair<List<String>, List<String>>> selectExamplesForValidation(int nExamples,
                                                                              ValidationExamplesSelectionPolicy selectionPolicy) {
        // Get feedback on most informative examples: 1- x->tables or
        // (x,y)->tables? 2- Based on entropy? Num of tables?
        List<Pair<List<String>, List<String>>> selectedKeys = new ArrayList<Pair<List<String>, List<String>>>();

        if (selectionPolicy == ValidationExamplesSelectionPolicy.X_MAX_NUM_TABLES) {
            Histogram<List<String>> keyToTableNum = new Histogram<List<String>>();
            for (List<String> k : knownExamples.keySet()) {
                HashSet<MultiColMatchCase> tables = new HashSet<>();
                for (List<String> v : keyToImages.get(k).keySet()) {
                    tables.addAll(keyToImages.get(k).get(v));
                }
                keyToTableNum.increment(k, (double) tables.size());
            }

            List<Pair<List<String>, Double>> sortedKeys = keyToTableNum.getCountsSorted();
            int keyCount = 0;
            for (int i = 0; keyCount < nExamples && i < sortedKeys.size(); i++) {
                List<String> k = sortedKeys.get(i).key;
                if (!validatedExamples.containsKey(k)) {
                    for (List<String> v : keyToImages.get(k).keySet()) {
                        Pair<List<String>, List<String>> pair = new Pair<List<String>, List<String>>(
                                k, v);
                        selectedKeys.add(pair);
                    }

                    keyCount++;
                }
            }
        } else if (selectionPolicy == ValidationExamplesSelectionPolicy.XY_MAX_NUM_TABLES) {
            Histogram<Pair<List<String>, List<String>>> xyToTableNum = new Histogram<Pair<List<String>, List<String>>>();
            for (List<String> k : knownExamples.keySet()) {
                for (List<String> v : keyToImages.get(k).keySet()) {
                    xyToTableNum.increment(new Pair<List<String>, List<String>>(k, v),
                            (double) keyToImages.get(k).get(v).size());
                }
            }
            List<Pair<Pair<List<String>, List<String>>, Double>> sortedKeys = xyToTableNum
                    .getCountsSorted();
            int pairCount = 0;
            for (int i = 0; pairCount < nExamples && i < sortedKeys.size(); i++) {
                Pair<List<String>, List<String>> pair = sortedKeys.get(i).key;
                if (!(validatedExamples.containsKey(pair.key) && validatedExamples.get(pair.key)
                        .containsKey(pair.value)) && !selectedKeys.contains(pair)) {
                    selectedKeys.add(pair);
                    pairCount++;
                }
            }
        } else if (selectionPolicy == ValidationExamplesSelectionPolicy.XY_MIN_NUM_TABLES) {
            Histogram<Pair<List<String>, List<String>>> xyToTableNum = new Histogram<Pair<List<String>, List<String>>>();
            for (List<String> k : knownExamples.keySet()) {
                for (List<String> v : keyToImages.get(k).keySet()) {
                    xyToTableNum.increment(new Pair<List<String>, List<String>>(k, v),
                            (double) keyToImages.get(k).get(v).size());
                }
            }
            List<Pair<Pair<List<String>, List<String>>, Double>> sortedKeys = xyToTableNum
                    .getCountsSorted();
            int pairCount = 0;
            for (int i = sortedKeys.size() - 1; pairCount < nExamples && i >= 0; i--) {
                Pair<List<String>, List<String>> pair = sortedKeys.get(i).key;
                if (!(validatedExamples.containsKey(pair.key) && validatedExamples.get(pair.key)
                        .containsKey(pair.value)) && !selectedKeys.contains(pair)) {
                    selectedKeys.add(pair);
                    pairCount++;
                }
            }
        } else if (selectionPolicy == ValidationExamplesSelectionPolicy.CLUSTERING) {
            for (List<String> k : knownExamples.keySet()) {
                List<Pair<List<String>, Double>> sortedList = knownExamples.get(k)
                        .getCountsSorted();
                selectedKeys.addAll(retrieveBadPairs(k, sortedList));
            }
        } else if (selectionPolicy == ValidationExamplesSelectionPolicy.TruthDiscovery) {
            for (List<String> k : knownExamples.keySet()) {
                List<Pair<List<String>, Double>> sortedList = knownExamples.get(k)
                        .getCountsSorted();
                selectedKeys.addAll(retrieveBadPairs(k, sortedList));
            }
        } else if (selectionPolicy == ValidationExamplesSelectionPolicy.XY_MAX_COVER) {
            // Greedy approximation algorithm
            HashSet<MultiColMatchCase> coveredTables = new HashSet<MultiColMatchCase>();

            int totalExampleNum = 0;
            for (List<String> k : knownExamples.keySet()) {
                totalExampleNum += knownExamples.get(k).getCountsUnsorted().size();
            }
            for (int i = 0; i < nExamples && i < totalExampleNum; i++) {
                Histogram<Pair<List<String>, List<String>>> xyToNewTablesNum = new Histogram<Pair<List<String>, List<String>>>();
                for (List<String> k : knownExamples.keySet()) {
                    for (List<String> v : keyToImages.get(k).keySet()) {
                        int newTablesNum = 0;
                        for (MultiColMatchCase table : keyToImages.get(k).get(v)) {
                            if (!coveredTables.contains(table)) {
                                newTablesNum++;
                            }
                        }

                        xyToNewTablesNum.increment(new Pair<List<String>, List<String>>(k, v),
                                (double) newTablesNum);
                    }
                }
                List<Pair<Pair<List<String>, List<String>>, Double>> sortedKeys = xyToNewTablesNum
                        .getCountsSorted();

                for (int j = 0; j < sortedKeys.size(); j++) {
                    Pair<List<String>, List<String>> pair = sortedKeys.get(j).key;

                    if (!(validatedExamples.containsKey(pair.key) && validatedExamples
                            .get(pair.key).containsKey(pair.value)) && !selectedKeys.contains(pair)) {
                        selectedKeys.add(pair);
                        coveredTables.addAll(keyToImages.get(pair.key).get(pair.value));
                        break;
                    }
                }
            }
        } else if (selectionPolicy == ValidationExamplesSelectionPolicy.XY_MIN_OVERLAP)// min
        // overlap
        // policy
        {
            HashSet<MultiColMatchCase> coveredTables = new HashSet<MultiColMatchCase>();

            int totalExampleNum = 0;
            for (List<String> k : knownExamples.keySet()) {
                totalExampleNum += knownExamples.get(k).getCountsUnsorted().size();
            }

            final Histogram<Pair<List<String>, List<String>>> xyToTablesNum = new Histogram<Pair<List<String>, List<String>>>();
            for (List<String> k : knownExamples.keySet()) {
                for (List<String> v : keyToImages.get(k).keySet()) {
                    xyToTablesNum.increment(new Pair<List<String>, List<String>>(k, v),
                            (double) keyToImages.get(k).get(v).size());
                }
            }

            for (int i = 0; i < nExamples && i < totalExampleNum; i++) {
                Histogram<Pair<List<String>, List<String>>> xyToCoveredTablesNum = new Histogram<Pair<List<String>, List<String>>>();
                for (List<String> k : knownExamples.keySet()) {
                    for (List<String> v : keyToImages.get(k).keySet()) {
                        int coveredTablesNum = 0;
                        for (MultiColMatchCase table : keyToImages.get(k).get(v)) {
                            if (coveredTables.contains(table)) {
                                coveredTablesNum++;
                            }
                        }
                        xyToCoveredTablesNum.increment(new Pair<List<String>, List<String>>(k, v),
                                (double) coveredTablesNum);
                    }
                }
                List<Pair<Pair<List<String>, List<String>>, Double>> sortedKeys = xyToCoveredTablesNum
                        .getCountsSorted();

                Collections.sort(sortedKeys,
                        new Comparator<Pair<Pair<List<String>, List<String>>, Double>>() {
                            @Override
                            public int compare(
                                    Pair<Pair<List<String>, List<String>>, Double> entry1,
                                    Pair<Pair<List<String>, List<String>>, Double> entry2) {
                                if (entry1.value.intValue() != entry2.value.intValue()) {
                                    return Integer.compare(entry1.value.intValue(),
                                            entry2.value.intValue());
                                } else {
                                    // System.out.print(xyToTablesNum.getScoreOf(entry1.key).intValue()
                                    // + "," +
                                    // xyToTablesNum.getScoreOf(entry2.key).intValue()
                                    // + " --> ");
                                    // System.out.println(Integer.compare(xyToTablesNum.getScoreOf(entry1.key).intValue(),
                                    // xyToTablesNum.getScoreOf(entry2.key).intValue()));
                                    return Integer.compare(xyToTablesNum.getScoreOf(entry1.key)
                                            .intValue(), xyToTablesNum.getScoreOf(entry2.key)
                                            .intValue());
                                }
                            }
                        });

                for (int j = 0; j < sortedKeys.size(); j++) {
                    Pair<List<String>, List<String>> pair = sortedKeys.get(j).key;

                    if (!(validatedExamples.containsKey(pair.key) && validatedExamples
                            .get(pair.key).containsKey(pair.value)) && !selectedKeys.contains(pair)) {
                        selectedKeys.add(pair);
                        coveredTables.addAll(keyToImages.get(pair.key).get(pair.value));
                        break;
                    }
                }

            }
        }
        // For debugging/experiments purposes only. Assumes that
        // GeneralTests.groundTruth is populated
        else if (selectionPolicy == ValidationExamplesSelectionPolicy.ORACLE_BAD_EXAMPLES) {
            HashSet<MultiColMatchCase> coveredTables = new HashSet<MultiColMatchCase>();

            int totalExampleNum = 0;
            for (List<String> k : knownExamples.keySet()) {
                totalExampleNum += knownExamples.get(k).getCountsUnsorted().size();
            }

            final Histogram<Pair<List<String>, List<String>>> xyToTablesNum = new Histogram<Pair<List<String>, List<String>>>();
            for (List<String> k : knownExamples.keySet()) {
                for (List<String> v : keyToImages.get(k).keySet()) {
                    xyToTablesNum.increment(new Pair<List<String>, List<String>>(k, v),
                            (double) keyToImages.get(k).get(v).size());
                }
            }

            List<Pair<Pair<List<String>, List<String>>, Double>> sortedKeys = xyToTablesNum
                    .getCountsSorted();

            for (int i = 0; i < nExamples && i < totalExampleNum; i++) {
                // take only the bad
                for (int j = 0; j < sortedKeys.size(); j++) {
                    Pair<List<String>, List<String>> pair = sortedKeys.get(j).key;

                    if ((!GeneralTests.groundTruth.containsKey(pair.key))
                            || (GeneralTests.groundTruth.containsKey(pair.key) && !GeneralTests.groundTruth
                            .get(pair.key).containsKey(pair.value))) {
                        if (!(validatedExamples.containsKey(pair.key) && validatedExamples.get(
                                pair.key).containsKey(pair.value))
                                && !selectedKeys.contains(pair)) {
                            selectedKeys.add(pair);
                            coveredTables.addAll(keyToImages.get(pair.key).get(pair.value));
                            break;
                        }
                    }
                }
            }

        } else if (selectionPolicy == ValidationExamplesSelectionPolicy.XY_MIN_SCORE) {
            Histogram<Pair<List<String>, List<String>>> xyToScore = new Histogram<Pair<List<String>, List<String>>>();
            for (List<String> k : answerToRating.keySet()) {
                for (List<String> v : answerToRating.get(k).getCountsUnsorted().keySet()) {
                    xyToScore.increment(new Pair<List<String>, List<String>>(k, v), answerToRating
                            .get(k).getScoreOf(v));
                }
            }

            List<Pair<Pair<List<String>, List<String>>, Double>> decreasingOrder = xyToScore
                    .getCountsSorted();

            for (int i = decreasingOrder.size() - 1, selectedKeysCount = 0; i >= 0
                    && selectedKeysCount < nExamples; i--) {
                Pair<List<String>, List<String>> pair = decreasingOrder.get(i).key;

                if (!(validatedExamples.containsKey(pair.key) && validatedExamples.get(pair.key)
                        .containsKey(pair.value)) && !selectedKeys.contains(pair)) {
                    selectedKeys.add(pair);
                    selectedKeysCount++;
                }
            }

        } else {
            throw new UnsupportedOperationException("The selection policy " + selectionPolicy
                    + " is not supported yet. Sorry for the inconvenience.");
        }
        return selectedKeys;
    }

    private Collection<? extends Pair<List<String>, List<String>>> retrieveBadPairs(
            List<String> key, List<Pair<List<String>, Double>> sortedList) {
        List<Pair<List<String>, Double>> goodList = new ArrayList<>();
        List<Pair<List<String>, Double>> badList = new ArrayList<>();
        List<Pair<List<String>, List<String>>> badPairs = new ArrayList<>();
        goodList.add(sortedList.get(0));
        double prevScore = 0;
        for (int i = 1; i < sortedList.size(); i++) {
            double firstSum = 0;
            double secondSum = 0;
            for (Pair<List<String>, Double> goodValue : goodList) {
                firstSum += goodValue.value;
            }
            for (int j = 0; j < goodList.size(); j++) {
                for (int k = j + 1; k < goodList.size(); k++) {
                    secondSum += Math.abs(goodList.get(j).value - goodList.get(k).value);
                }

            }
            double score = firstSum - secondSum;
            if (score <= prevScore) {
                break;
            }
            prevScore = score;
            goodList.add(sortedList.get(i));
        }
        badList.addAll(sortedList);
        badList.removeAll(goodList);
        for (Pair<List<String>, Double> goodValue : badList) {
            badPairs.add(new Pair<List<String>, List<String>>(key, goodValue.key));
        }

        return badPairs;
    }

    /**
     * Computes the rating of a table (i.e., a match case)
     *
     * @param MultiColMatchCase
     * @return
     */
    private double recomputeRating(MultiColMatchCase MultiColMatchCase) {
        double prior = tableToPrior.get(MultiColMatchCase);

        double goodSum = 0;
        double totalSum = 0;
        double badSum = 0;

        HashSet<List<String>> coveredKeys = new HashSet<>();

        for (Pair<List<String>, List<String>> entry : tableToAnswers.get(MultiColMatchCase)) {
            List<String> k = entry.key;
            List<String> v = entry.value;

            coveredKeys.add(k);

            if (goodExamples.containsKey(k)) {
                if (goodExamples.get(k).containsKey(v)) {
                    goodSum += knownExamples.get(k).getScoreOf(v);
                    totalSum += knownExamples.get(k).getScoreOf(v);
                } else {
                    totalSum += goodExamples.get(k).size();
                    // totalSum +=
                    // knownExamples.get(k).getCountsSorted().get(0).value;
                }
            }

            if (badExamples.containsKey(k) && badExamples.get(k).containsKey(v)) {
                // totalSum += BAD_EXAMPLE_WEIGHT; // TODO: decide on weight?
                badSum++;
                totalSum++;
            }

        }

        double unseenWeightSum = 0;
        for (List<String> k : goodExamples.keySet()) {
            if (!coveredKeys.contains(k)) {
                unseenWeightSum += goodExamples.get(k).size();
                // unseenWeightSum++;
            }
        }
        double instanceBasedRating = (prior * goodSum - (1 - prior) * 2 * badSum)
                * SMOOTHING_FACTOR_ALPHA / (totalSum + (1 - prior) * unseenWeightSum);

        // return instanceBasedRating;
        double schemaScore = tableToSchemaScore.get(MultiColMatchCase);

        // double rating = (instanceBasedRating + schemaScore*schemaMatchWeight)
        // / (1 + schemaMatchWeight);
        double rating = instanceBasedRating;
        return rating;
    }

    /**
     * Expectation Maximisation to score answers
     *
     * @param MultiColMatchCase
     * @return
     */
    private double computeRating(MultiColMatchCase MultiColMatchCase) {
        double prior = tableToPrior.get(MultiColMatchCase);
        double tf_idf = 0.0;

        double goodSum = 0;
        double totalSum = 0;
        double badSum = 0;

        HashSet<List<String>> coveredKeys = new HashSet<>();
        ArrayList<String> context = MultiColMatchCase.getContext();

        for (Pair<List<String>, List<String>> entry : tableToAnswers.get(MultiColMatchCase)) {
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
                // totalSum += BAD_EXAMPLE_WEIGHT; // TODO: decide on weight?
                badSum++;
                totalSum++;
            }

            // Calculate tf idf
            for (String s : k) {
                if (context.contains(s)) {
                    if (termToIdf.get(s) == null) {
                        continue;
                    }
                    tf_idf += termToIdf.get(s);
                }
            }
            for (String s : v) {
                if (context.contains(s)) {
                    if (termToIdf.get(s) == null) {
                        continue;
                    }
                    tf_idf += termToIdf.get(s);
                }
            }

        }

        double unseenWeightSum = 0;
        for (List<String> k : knownExamples.keySet()) {
            if (!coveredKeys.contains(k)) {
                unseenWeightSum += knownExamples.get(k).getTotalCount();
                // unseenWeightSum++;
            }
        }

        //Combine Openrank and TF_IDF
        prior = Math.min(prior * (1 / 1 + Math.exp(-0.1 * tf_idf)), 1.0);

        double instanceBasedRating = (prior * goodSum - (1 - prior) * 2 * badSum)
                * SMOOTHING_FACTOR_ALPHA / (totalSum + (1 - prior) * unseenWeightSum);

        // return instanceBasedRating;
        double schemaScore = tableToSchemaScore.get(MultiColMatchCase);

        // double rating = (instanceBasedRating + schemaScore*schemaMatchWeight)
        // / (1 + schemaMatchWeight);
        double rating = instanceBasedRating;
        return rating;
    }

    /**
     * Expectation Maximisation to score answers
     *
     * @param MultiColMatchCase
     * @return
     */
    private double similarComputeRating(MultiColMatchCase MultiColMatchCase, List<String> inputs, List<String> outputs) {
        double prior = tableToPrior.get(MultiColMatchCase);
        double tf_idf = 0.0;

        double goodSum = 0;
        double totalSum = 0;
        double badSum = 0;

        HashSet<List<String>> coveredKeys = new HashSet<>();
        ArrayList<String> context = MultiColMatchCase.getContext();

        for (Pair<List<String>, List<String>> entry : tableToAnswers.get(MultiColMatchCase)) {
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

            for (String key : k) {
                if (inputs.contains(key)) {
                    for (String value : v) {
                        if (outputs.contains(value)) {
                            goodSum += 1.0;
                            totalSum += 1.0;
                        }
                    }
                }
            }

            if (badExamples.containsKey(k) && badExamples.get(k).containsKey(v)) {
                // totalSum += BAD_EXAMPLE_WEIGHT; // TODO: decide on weight?
                badSum++;
                totalSum++;
            }

            // Calculate tf idf
            for (String s : k) {
                if (context.contains(s)) {
                    if (termToIdf.get(s) == null) {
                        continue;
                    }
                    tf_idf += termToIdf.get(s);
                }
            }
            for (String s : v) {
                if (context.contains(s)) {
                    if (termToIdf.get(s) == null) {
                        continue;
                    }
                    tf_idf += termToIdf.get(s);
                }
            }

            for (String s : inputs) {
                if (context.contains(s)) {
                    if (termToIdf.get(s) == null) {
                        continue;
                    }
                    tf_idf += termToIdf.get(s);
                }
            }
            for (String s : outputs) {
                if (context.contains(s)) {
                    if (termToIdf.get(s) == null) {
                        continue;
                    }
                    tf_idf += termToIdf.get(s);
                }
            }

        }

        double unseenWeightSum = 0;
        for (List<String> k : knownExamples.keySet()) {
            if (!coveredKeys.contains(k)) {
                unseenWeightSum += knownExamples.get(k).getTotalCount();
                // unseenWeightSum++;
            }
        }

        for (String k : inputs) {
            if (!coveredKeys.contains(Collections.singletonList(k))) {
                unseenWeightSum += 1.0;
                // unseenWeightSum++;
            }
        }

        //Combine Openrank and TF_IDF
        prior = Math.min(prior * (1 / 1 + Math.exp(-0.1 * tf_idf)), 1.0);

        double instanceBasedRating = (prior * goodSum - (1 - prior) * 2 * badSum)
                * SMOOTHING_FACTOR_ALPHA / (totalSum + (1 - prior) * unseenWeightSum);

        return instanceBasedRating;
    }

    private double computeSchemaMatch(String[] h1, String[] h2) {
        String[] headers1 = inputTable.getColumnMapping().getColumnNames(colsFrom);
        String[] headers2 = inputTable.getColumnMapping().getColumnNames(colsTo);

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
     * Just a wrapper for the original method, that returns only a single (the
     * top) answer for each key
     */
    public StemMultiColMap<Pair<List<String>, Double>> transformFunctional() throws Exception {
        iterate();
        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> r = getCurrentAnswer();

        StemMultiColMap<Pair<List<String>, Double>> result = new StemMultiColMap<Pair<List<String>, Double>>();

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
     * Just a wrapper for the original method, that returns only a single (the
     * top) answer for each key
     */
    public void transformFunctionalSyntactical() throws Exception {
        iterateSyntactic();
    }


    public void printResult(StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImages) {
        for (List<String> k : keyToImages.keySet()) {
            System.out.println(k);

            StemMultiColMap<HashSet<MultiColMatchCase>> imageVals = keyToImages.get(k);
            for (List<String> v : imageVals.keySet()) {
                System.out.println("--" + v);
                System.out.println("\t" + keyToImages.get(k).get(v));
            }
            System.out.println("================================================");
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
    public HashMap<List<String>, Pair<List<Double>, List<Double>>> checkKnownTupleMapping(Table t,
                                                                                          int[] anchorCols, int[] depCols, StemMultiColMap<StemMultiColHistogram> knownExamples,
                                                                                          boolean closedWorld) {
        HashMap<List<String>, Pair<List<Double>, List<Double>>> keyToGoodAndBad = new StemMultiColMap<Pair<List<Double>, List<Double>>>();
        if (!fuzzyMatching) {
            StemMultiColMap<StemMultiColHistogram> tableAsTree = createMapFromTable(t, anchorCols,
                    depCols);
            if (consolidation == ConsolidationMethod.FUNCTIONAL) {

                for (List<String> k : tableAsTree.keySet()) {
                    if (validatedExamples.containsKey(k)) {
                        List<Double> good = new ArrayList<Double>();
                        List<Double> bad = new ArrayList<Double>();

                        if (knownExamples.containsKey(k)) {

                            for (List<String> v : tableAsTree.get(k).getCountsUnsorted().keySet()) {
                                if (knownExamples.get(k).containsKey(v)) {
                                    good.add(knownExamples.get(k).getScoreOf(v));
                                } else {
                                    // bad.add(knownExamples.get(k).getCountsSorted().get(0).value);
                                    bad.add(1.0);
                                    setBadExample(k, v);
                                }

                            }
                        }
                        if (badExamples.containsKey(k)) {
                            for (List<String> v : tableAsTree.get(k).getCountsUnsorted().keySet()) {
                                if (badExamples.get(k).containsKey(v))
                                    bad.add(1.0);
                            }
                        }
                        keyToGoodAndBad.put(k, new Pair<List<Double>, List<Double>>(good, bad));
                    }
                }
            }
            if (consolidation == ConsolidationMethod.NON_FUNCTIONAL) {

                for (List<String> k : tableAsTree.keySet()) {
                    List<Double> good = new ArrayList<Double>();
                    List<Double> bad = new ArrayList<Double>();
                    if (knownExamples.containsKey(k)) {

                        for (List<String> v : tableAsTree.get(k).getCountsUnsorted().keySet()) {
                            if (knownExamples.get(k).containsKey(v)) {
                                good.add(knownExamples.get(k).getScoreOf(v));
                            }
                        }

                    }
                    if (badExamples.containsKey(k)) {
                        for (List<String> v : tableAsTree.get(k).getCountsUnsorted().keySet()) {
                            if (badExamples.get(k).containsKey(v)) {
                                bad.add(1.0);
                            }
                        }
                    }
                    if (bad.size() > 0 || good.size() > 0)
                        keyToGoodAndBad.put(k, new Pair<List<Double>, List<Double>>(good, bad));

                }

            }
        } else // With fuzzy matching
        {

            if (!(queryBuilder instanceof VerticaMultiColSimilarityQuerier)) {
                throw new IllegalStateException(
                        "VerticaSimilarityQuerier must be used with fuzzy matching.");
            }

            HashMap<List<String>, Histogram<List<String>>> tableAsTree = createExactMapFromTable(t,
                    anchorCols, depCols);

            HashMap<String, Collection<String>> valToSimilarValForms = ((VerticaMultiColSimilarityQuerier) queryBuilder).valToSimilarValForms;

            for (List<String> k : tableAsTree.keySet()) {
                List<Double> good = new ArrayList<Double>();
                List<Double> bad = new ArrayList<Double>();

                if (knownExamples.containsKey(k)) {

                    for (List<String> v : tableAsTree.get(k).getCountsUnsorted().keySet()) {
                        if (knownExamples.get(k).containsKey(v)) {
                            good.add(knownExamples.get(k).getScoreOf(v));
                        } else {

                            for (List<String> standardKey : exactOriginalExamples.keySet()) {
                                if (StemMultiColMap.stemList(standardKey).equals(
                                        StemMultiColMap.stemList(k))) {
                                    List<String> standardValForm = null;

                                    for (List<String> standardV : exactOriginalExamples
                                            .get(standardKey)) {
                                        boolean allInRangeV = true;
                                        for (int i = 0; i < standardV.size(); i++) {
                                            if (!valToSimilarValForms.get(standardV.get(i))
                                                    .contains(v.get(i))) {
                                                allInRangeV = false;
                                                break;
                                            }
                                        }
                                        if (allInRangeV) {
                                            standardValForm = standardV;
                                        }
                                    }

                                    // TODO: If not in range, penalize
                                    if (standardValForm != null) {
                                        good.add(Similarity.similarity(v, standardValForm));
                                    } else {
                                        // bad.add(knownExamples.get(k).getCountsSorted().get(0).value);
                                        bad.add(1.0);
                                    }
                                }
                            }
                        }
                    }

                    keyToGoodAndBad.put(k, new Pair<List<Double>, List<Double>>(good, bad));
                } else // check if original and within distance
                {
                    for (List<String> standardKey : exactOriginalExamples.keySet()) {
                        boolean allInRangeK = true;
                        for (int i = 0; i < standardKey.size(); i++) {
                            if (!valToSimilarValForms.get(standardKey.get(i)).contains(k.get(i))) {
                                allInRangeK = false;
                                break;
                            }
                        }
                        if (allInRangeK) {
                            for (List<String> v : tableAsTree.get(k).getCountsUnsorted().keySet()) {
                                List<String> standardValForm = null;

                                for (List<String> standardV : exactOriginalExamples
                                        .get(standardKey)) {
                                    boolean allInRangeV = true;
                                    for (int i = 0; i < standardV.size(); i++) {
                                        if (!valToSimilarValForms.get(standardV.get(i)).contains(
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

                                // TODO: If not in range, penalize
                                if (standardValForm != null) {
                                    good.add(Similarity.similarity(v, standardValForm)
                                            * Similarity.similarity(k, standardKey));
                                } else {
                                    // bad.add(knownExamples.get(k).getCountsSorted().get(0).value);
                                    bad.add(Similarity.similarity(k, standardKey));
                                }
                            }
                            keyToGoodAndBad.put(k, new Pair<List<Double>, List<Double>>(good, bad));
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
    public HashMap<List<String>, Pair<Double, Double>> checkLocalCompleteness(Table t,
                                                                              int[] anchorCols, int[] depCols, StemMultiColMap<StemMultiColHistogram> knownExamples,
                                                                              boolean closedWorld) {
        HashMap<List<String>, Pair<Double, Double>> keyToPresentMissingValues = new HashMap<List<String>, Pair<Double, Double>>();

        if (!fuzzyMatching) {
            StemMultiColMap<StemMultiColHistogram> tableAsTree = createMapFromTable(t, anchorCols,
                    depCols);

            for (List<String> k : tableAsTree.keySet()) {
                if (knownExamples.containsKey(k)) {
                    double present = 0;
                    double absent = 0;

                    for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                        // or weight?
                        // double w = 1.0;
                        double w = knownExamples.get(k).getScoreOf(v);

                        if (tableAsTree.get(k).containsKey(v)) {
                            present += w;
                        } else {
                            absent += w;
                        }
                    }

                    keyToPresentMissingValues.put(k, new Pair<Double, Double>(present, absent));

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
        double localCompletenessAverageScore = presentSum / (presentSum + absentSum);

        if (closedWorld) {
            return goodCount >= COVERAGE_THRESHOLD && correctness >= QUALITY_THRESHOLD;
        } else if (localCompleteness) {
            return goodCount >= COVERAGE_THRESHOLD
                    && localCompletenessAverageScore >= QUALITY_THRESHOLD;
        } else {
            return goodCount >= COVERAGE_THRESHOLD;
        }
    }

    public static StemMultiColMap<StemMultiColHistogram> createMapFromTable(Table table,
                                                                            int[] colsFrom, int[] colsTo) {
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

    private static ArrayList<FunctionalDependency> findFDs(Table table) throws Exception {
        TANEjava tane = new TANEjava(table, FD_ERROR);
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
    private static ArrayList<FunctionalDependency> findFDsWithColAsLHS(Table table, int anchorColIdx)
            throws Exception {
        ArrayList<FunctionalDependency> allFDs = findFDs(table);

        ArrayList<FunctionalDependency> fdsWithColInLHS = new ArrayList<>();

        for (FunctionalDependency fd : allFDs) {
            if (fd.getX().size() == 1 && fd.getX().contains(anchorColIdx)) {
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
    private static ArrayList<FunctionalDependency> findFDsWithColInLHS(Table table, int anchorColIdx)
            throws Exception {
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
                if ((v == null && bestAnswer == null) || (v != null && v.equals(bestAnswer))) {
                    normalizedDistrib.increment(v,
                            0.5 + (answers.get(bestAnswer).size() * 0.5 / tableToAnswers.size()));
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

        private AbstractColumnMatcher[] colMatchersFrom;
        private AbstractColumnMatcher[] colMatchersTo;
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
                MultiColTableLoader tableLoader, MultiColMatchCase multiColMatchCase,
                AbstractColumnMatcher[] colMatchersFrom, AbstractColumnMatcher[] colMatchersTo,
                boolean useNewlyDiscoveredKeys, boolean closedWorld, boolean useWeightedExamples,
                boolean localCompleteness) {
            super();
            this.keyToImages = keyToImages;
            this.tableLoader = tableLoader;
            this.multiColMatchCase = multiColMatchCase;
            this.colMatchersFrom = colMatchersFrom;
            this.colMatchersTo = colMatchersTo;
            this.useNewlyDiscoveredKeys = useNewlyDiscoveredKeys;
            this.closedWorld = closedWorld;
            this.useWeightedExamples = useWeightedExamples;
            this.localCompleteness = localCompleteness;
        }

        @Override
        public void run() {
            try {
                String tableID = multiColMatchCase.tableID;
                int[] colsF = multiColMatchCase.col1;
                int[] colsT = multiColMatchCase.col2;

                Table webTable = tableLoader.loadTable(multiColMatchCase);

                HashMap<List<String>, Pair<List<Double>, List<Double>>> checkResult = checkKnownTupleMapping(
                        webTable, colsF, colsT, knownExamples, closedWorld);

                HashMap<List<String>, Pair<Double, Double>> localCompletenessCheck = checkLocalCompleteness(
                        webTable, colsF, colsT, knownExamples, closedWorld);

                if (assessTupleMapping(checkResult, localCompletenessCheck, closedWorld,
                        useWeightedExamples, localCompleteness)) {
                    // Transformation found

                    for (Tuple tup : webTable.getTuples()) {
                        List<String> k = tup.getValuesOfCells(colsF);
                        List<String> v = tup.getValuesOfCells(colsT); // the
                        // transformed
                        // value

                        k = StemMultiColMap.stemList(k);
                        v = StemMultiColMap.stemList(v);

                        if (k != null) {
                            if (!keyToImages.containsKey(k) && !useNewlyDiscoveredKeys) {
                                continue;
                            }

                            if (!result.containsKey(k)) {
                                result.put(k, new StemMultiColMap<HashSet<MultiColMatchCase>>());
                            }
                            if (!result.get(k).containsKey(v)) {
                                result.get(k).put(v, new HashSet<MultiColMatchCase>());
                            }

                            if (!tableToAnswersPartial.containsKey(multiColMatchCase)) {
                                tableToAnswersPartial.put(multiColMatchCase,
                                        new HashSet<Pair<List<String>, List<String>>>());
                            }
                            tableToAnswersPartial.get(multiColMatchCase).add(
                                    new Pair<List<String>, List<String>>(k, v));

                            result.get(k).get(v).add(multiColMatchCase);
                        }
                    }
                    //FIXME confidence vs openrank
                    if (!useOpenrank) {
                        tableToPriorPartial.put(multiColMatchCase, webTable.confidence);
                    } else {
                        tableToPriorPartial.put(multiColMatchCase, webTable.openrank);
                    }

                    double schemaMatchScore;
                    if (webTable.hasHeader()) {
                        String[] h1 = webTable.getColumnMapping().getColumnNames(
                                multiColMatchCase.col1);
                        String[] h2 = webTable.getColumnMapping().getColumnNames(
                                multiColMatchCase.col2);

                        schemaMatchScore = computeSchemaMatch(h1, h2);
                    } else {
                        schemaMatchScore = schemaMatchPrior;
                    }
                    tableToSchemaPriorPartial.put(multiColMatchCase, schemaMatchScore);
                } else { // TODO: indirect coverage?
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    public void setGoodExample(List<String> k, List<String> v) {
        if (!knownExamples.containsKey(k)) {
            knownExamples.put(k, new StemMultiColHistogram());
        }
        if (!goodExamples.containsKey(k)) {
            goodExamples.put(k, new StemMultiColMap<HashSet<MultiColMatchCase>>());
        }
        if (!goodExamples.get(k).containsKey(v)) {
            goodExamples.get(k).put(v, new HashSet<MultiColMatchCase>());
        }
        knownExamples.get(k).setScoreOf(v, initialGoodExampleImportance);
        tableToAnswers.get(QUERY_TABLE_ID).add(new Pair<List<String>, List<String>>(k, v));

        if (!keyToImages.containsKey(k)) {
            keyToImages.put(k, new StemMultiColMap<HashSet<MultiColMatchCase>>());
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
            badExamples.put(k, new StemMultiColMap<HashSet<MultiColMatchCase>>());
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
                for (MultiColMatchCase MultiColMatchCase : keyToImages.get(k).get(v)) {
                    tableToCount.increment(MultiColMatchCase.tableID);
                }
            }
        }

        for (String tableID : tableToCount.getCountsUnsorted().keySet()) {
            double prior = VerticaBatchTableLoader.getPrior(tableID);
            VerticaBatchTableLoader.setPrior(tableID,
                    prior + (1 - prior) * tableToCount.getScoreOf(tableID) * SMOOTHING_FACTOR_ALPHA
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
    public MultiColMatchCase getBestTableForAnswer(List<String> k, List<String> v) {
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

    public HashMap<List<String>, Histogram<List<String>>> getInputTable() {
        return createExactMapFromTable(inputTable, colsFrom, colsTo);
    }

    public boolean noMoreTablesLoaded() {
        return noTablesLoaded;
    }

    public void runTruthDiscovery() {
        HashMap<Pair<List<String>, List<String>>, Boolean> answerToTF = new HashMap<Pair<List<String>, List<String>>, Boolean>();
        HashMap<Pair<List<String>, List<String>>, Boolean> answerToOc = new HashMap<Pair<List<String>, List<String>>, Boolean>();
        TObjectIntHashMap<MultiColMatchCase> n_claim_tp = new TObjectIntHashMap<MultiColMatchCase>();
        TObjectIntHashMap<MultiColMatchCase> n_claim_tn = new TObjectIntHashMap<MultiColMatchCase>();
        TObjectIntHashMap<MultiColMatchCase> n_claim_fp = new TObjectIntHashMap<MultiColMatchCase>();
        TObjectIntHashMap<MultiColMatchCase> n_claim_fn = new TObjectIntHashMap<MultiColMatchCase>();
        Random rand = new Random();
        BetaDistribution betaRandnegative = new BetaDistribution(10, 1000);

        for (List<String> key : keyToImages.keySet()) {
            StemMultiColMap<HashSet<MultiColMatchCase>> answers = keyToImages.get(key);
            for (List<String> v : answers.keySet()) {
                boolean truthLabel = true;
                Pair<List<String>, List<String>> fact = new Pair<List<String>, List<String>>(key, v);
                if (knownExamples.containsKey(key)
                        && knownExamples.get(key).getCountsUnsorted().containsKey(v)) {
                    answerToTF.put(fact, truthLabel);
                    answerToOc.put(fact, truthLabel);
                    for (MultiColMatchCase claim : answers.get(v)) {
                        n_claim_tp.adjustOrPutValue(claim, 1, 1);
                    }
                } else {
                    // sample

                    if (rand.nextDouble() < 0.5) {
                        truthLabel = false;
                    }
                    answerToTF.put(fact, truthLabel);

                    for (MultiColMatchCase claim : answers.get(v)) {
                        if (truthLabel) {
                            if (rand.nextDouble() < 0.5) {
                                n_claim_fn.adjustOrPutValue(claim, 1, 1);
                                answerToOc.put(fact, false);
                            } else {
                                n_claim_tp.adjustOrPutValue(claim, 1, 1);
                                answerToOc.put(fact, true);
                            }
                        } else {
                            boolean oc = true;
                            double phi = betaRandnegative.sample();
                            BinomialDistribution binDist = new BinomialDistribution(100, phi);
                            if (binDist.sample() < 0.5) {
                                n_claim_tn.adjustOrPutValue(claim, 1, 1);
                                answerToOc.put(fact, false);
                            } else {
                                n_claim_fp.adjustOrPutValue(claim, 1, 1);
                                answerToOc.put(fact, true);
                            }
                        }
                    }
                }
            }
        }
        TObjectDoubleMap<Pair<List<String>, List<String>>> answer_p_tf = new TObjectDoubleHashMap<Pair<List<String>, List<String>>>();
        TObjectDoubleMap<Pair<List<String>, List<String>>> answer_p_1_tf = new TObjectDoubleHashMap<Pair<List<String>, List<String>>>();
        for (int i = 0; i < 20; i++) {
            for (List<String> key : keyToImages.keySet()) {
                StemMultiColMap<HashSet<MultiColMatchCase>> answers = keyToImages.get(key);
                for (List<String> v : answers.keySet()) {
                    Pair<List<String>, List<String>> fact = new Pair<List<String>, List<String>>(
                            key, v);
                    answer_p_tf.put(fact, 10.0);// initialize with beta values
                    answer_p_1_tf.put(fact, 10);
                    boolean tf = answerToTF.get(fact);
                    boolean oc = answerToOc.get(fact);
                    int alpha_tf_oc = tf ? 50 : oc ? 10 : 1000;
                    int alpha_tf_1 = tf ? 50 : 10;
                    int alpha_tf_0 = tf ? 50 : 1000;
                    int alpha_1_tf_oc = (!tf) ? oc ? 10 : 1000 : 50;
                    int alpha_1_tf_1 = (!tf) ? 10 : 50;
                    int alpha_1_tf_0 = (!tf) ? 1000 : 50;
                    double p_tf = answer_p_tf.get(fact);
                    double p_1_tf = answer_p_1_tf.get(fact);
                    for (MultiColMatchCase claim : answers.get(v)) {
                        int n_claim_tf_oc = getCorrectNValue(answerToOc, answerToTF, fact, claim,
                                n_claim_tp, n_claim_tn, n_claim_fp, n_claim_fn, true);
                        int n_claim_1_tf_oc = getCorrectNValue(answerToOc, answerToTF, fact, claim,
                                n_claim_tp, n_claim_tn, n_claim_fp, n_claim_fn, false);

                        int n_claim_tf_1 = answerToTF.get(fact) ? n_claim_tp.get(claim)
                                : n_claim_fp.get(fact);
                        int n_claim_1_tf_1 = (!answerToTF.get(fact)) ? n_claim_fp.get(claim)
                                : n_claim_tp.get(claim);

                        int n_claim_tf_0 = answerToTF.get(fact) ? n_claim_fn.get(claim)
                                : n_claim_tn.get(fact);
                        int n_claim_1_tf_0 = answerToTF.get(fact) ? n_claim_tn.get(claim)
                                : n_claim_fn.get(claim);

                        p_tf = (p_tf * (n_claim_tf_oc - 1 + alpha_tf_oc))
                                / (n_claim_tf_1 + n_claim_tf_0 - 1 + alpha_tf_1 + alpha_tf_0);
                        p_1_tf = (p_1_tf * (n_claim_1_tf_oc + alpha_1_tf_oc))
                                / (n_claim_1_tf_1 + n_claim_1_tf_0 - 1 + alpha_1_tf_1 + alpha_1_tf_0);
                    }

                    if (rand.nextDouble() < (p_1_tf / (p_tf + p_1_tf))) {
                        tf = !tf;
                        answerToTF.put(fact, tf);

                        for (MultiColMatchCase claim : answers.get(v)) {
                            if (oc) {
                                if (tf) {
                                    n_claim_fp.put(claim, n_claim_fp.get(claim) - 1);
                                    n_claim_tp.adjustOrPutValue(claim, 1, 1);
                                } else {
                                    n_claim_tp.put(claim, n_claim_tp.get(claim) - 1);
                                    n_claim_fp.adjustOrPutValue(claim, 1, 1);
                                }
                            } else {
                                if (tf) {
                                    n_claim_fn.put(claim, n_claim_fn.get(claim) - 1);
                                    n_claim_tn.adjustOrPutValue(claim, 1, 1);
                                } else {
                                    n_claim_tn.put(claim, n_claim_tn.get(claim) - 1);
                                    n_claim_fn.adjustOrPutValue(claim, 1, 1);
                                }

                            }
                        }

                    }

                }

            }

        }
        double alpha_1_1 = 50;
        double alpha_1_0 = 50;
        for (Pair<List<String>, List<String>> fact : answerToTF.keySet()) {
            double score = 1.0;
            boolean oc = answerToOc.get(fact);
            for (MultiColMatchCase claim : keyToImages.get(fact.key).get(fact.value)) {
                double n_f_sc_1_1 = 0;
                double n_f_sc_1_0 = 0;
                double n_f_sc_1_oc = 0;
                double alpha_1_oc = 50;
                if (oc) {
                    n_f_sc_1_1 = n_claim_tp.get(claim) < 1 ? 0 : n_claim_tp.get(claim) - 1;
                    n_f_sc_1_0 = n_claim_tn.get(claim) < 0 ? 0 : n_claim_tn.get(claim);
                    n_f_sc_1_oc = n_f_sc_1_1;
                } else {
                    n_f_sc_1_1 = n_claim_tp.get(claim) < 0 ? 0 : n_claim_tp.get(claim);
                    n_f_sc_1_0 = n_claim_tn.get(claim) < 1 ? 0 : n_claim_tn.get(claim) - 1;
                    n_f_sc_1_oc = n_f_sc_1_0;
                }

                score = score * (n_f_sc_1_oc + alpha_1_oc)
                        / (n_f_sc_1_1 + n_f_sc_1_0 + alpha_1_1 + alpha_1_0);
            }

            StemMultiColHistogram distr;

            if (knownExamples.containsKey(fact.key)) {
                distr = knownExamples.get(fact.key);
            } else {
                distr = new StemMultiColHistogram();
            }
            distr.increment(fact.value, score);
            answerToRating.put(fact.key, distr);
            if (score > 0.5) {
                knownExamples.put(fact.key, distr);
            }
            // score = 50* ;
        }
    }

    private int getCorrectNValue(HashMap<Pair<List<String>, List<String>>, Boolean> answerToOc,
                                 HashMap<Pair<List<String>, List<String>>, Boolean> answerToTF,
                                 Pair<List<String>, List<String>> fact, MultiColMatchCase claim,
                                 TObjectIntHashMap<MultiColMatchCase> n_claim_tp,
                                 TObjectIntHashMap<MultiColMatchCase> n_claim_tn,
                                 TObjectIntHashMap<MultiColMatchCase> n_claim_fp,
                                 TObjectIntHashMap<MultiColMatchCase> n_claim_fn, boolean b) {
        if (answerToTF.containsKey(fact) == b) {
            if (answerToOc.containsKey(fact)) {
                return n_claim_tp.get(claim);
            } else {
                return n_claim_fn.get(claim);
            }
        } else {
            if (answerToOc.containsKey(fact)) {
                return n_claim_fp.get(claim);
            } else {
                return n_claim_tn.get(claim);
            }
        }
    }

    public void iterateBayesian() throws IOException, ParseException, InterruptedException,
            ExecutionException, SQLException {

        noTablesLoaded = false;
        int iter = 0;
        StemMultiColMap<StemMultiColHistogram> lastKnownExamples = new StemMultiColMap<StemMultiColHistogram>();
        StemMultiColMap<String> foundXs = new StemMultiColMap<String>();

        boolean doQueries = iter < inductiveIters;

        done = false;
        message = "Initializing";

        answerToRating.putAll(knownExamples);
        HashSet<String> allTableIDs = new HashSet<String>();
        while (true) {
            // Clone knownExamples
            for (List<String> k : knownExamples.keySet()) {
                StemMultiColHistogram s = new StemMultiColHistogram();
                for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                    s.increment(v, knownExamples.get(k).getScoreOf(v));
                }
                lastKnownExamples.put(k, s);
            }
            // -------------------

            int processedTablesCount = 0;
            StemMultiColMap<String> newlyFoundXs = new StemMultiColMap<String>();

            if (doQueries) {
                message = "Iteration " + (iter + 1) + " - querying for tables";

                ArrayList<MultiColMatchCase> tablesToCheck = queryBuilder.findTables(knownExamples,
                        COVERAGE_THRESHOLD);
                if (tableLoader instanceof BatchTableLoader) {
                    HashSet<String> tableIDs = new HashSet<String>();
                    for (MultiColMatchCase multiColMatchCase : tablesToCheck) {
                        if (allTableIDs.contains(multiColMatchCase.tableID)
                                || seenTriples.contains(multiColMatchCase)) {
                            continue;
                        }
                        tableIDs.add(multiColMatchCase.tableID);
                        allTableIDs.add(multiColMatchCase.tableID);
                    }
                    if (tableIDs.isEmpty()) {
                        System.out.println("No new tables to load");
                        noTablesLoaded = true;
                        return;
                    }

                    ((BatchTableLoader) tableLoader).loadTables(tableIDs.toArray(new String[0]));
                }
                message = "Iteration " + (iter + 1) + " - checking tables";

                ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREADS);
                ArrayList<TableChecker> tableCheckers = new ArrayList<WTTransformerMultiColSets.TableChecker>();

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
                        TableChecker tc = new TableChecker(keyToImages, tableLoader,
                                MultiColMatchCase, colMatchersFrom, colMatchersTo,
                                useNewlyDiscoveredKeys, closedWorld, useWeightedExamples,
                                localCompleteness);
                        tableCheckers.add(tc);
                        threadPool.submit(tc);
                    }

                }

                threadPool.shutdown();
                while (true) {
                    try {
                        if (threadPool.awaitTermination(5, TimeUnit.HOURS)) {
                            break;
                        }
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }

                processedTablesCount += tableCheckers.size();

                message = "Iteration " + (iter + 1) + " - consolidating results";

                for (TableChecker tc : tableCheckers) {
                    if (tc.tableToPriorPartial.size() > 0) {
                        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> partialResult = tc.result;
                        tableToPrior.putAll(tc.tableToPriorPartial);
                        tableToAnswers.putAll(tc.tableToAnswersPartial);
                        tableToSchemaScore.putAll(tc.tableToSchemaPriorPartial);

                        // TODO : is it promising?
                        // MultiColMatchCase MultiColMatchCase =
                        // tc.getMultiColMatchCase();
                        // tableToRating.put(MultiColMatchCase,
                        // computeRating(MultiColMatchCase));

                        for (List<String> k : partialResult.keySet()) {
                            if (!keyToImages.containsKey(k)) {
                                if (!useNewlyDiscoveredKeys) {
                                    continue;
                                }
                                keyToImages.put(k,
                                        new StemMultiColMap<HashSet<MultiColMatchCase>>());
                            }
                            for (List<String> v : partialResult.get(k).keySet()) {
                                if (badExamples.containsKey(k) && badExamples.get(k).containsKey(v)) {
                                    // add all its witness tables
                                    badExamples.get(k).get(v).addAll(partialResult.get(k).get(v));
                                } else {
                                    if (!keyToImages.get(k).containsKey(v)) {
                                        keyToImages.get(k).put(v, new HashSet<MultiColMatchCase>());
                                    }
                                    keyToImages.get(k).get(v).addAll(partialResult.get(k).get(v));
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

            // Maximization: revise table ratings
            tableToRating.put(QUERY_TABLE_ID, 1.0);

            runTruthDiscovery();
            if (newlyFoundXs.isEmpty()) {
                break;
            }

            message = "Iteration " + (iter + 1) + " - adjusting answer scores";
        }
    }

}
