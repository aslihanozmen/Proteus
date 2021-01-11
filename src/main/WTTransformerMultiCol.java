package main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import model.BatchTableLoader;
import model.Table;
import model.Tuple;
import model.VerticaBatchTableLoader;
import model.columnMapping.AbstractColumnMatcher;
import model.multiColumn.MultiColTableLoader;

import org.apache.commons.cli.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.FSDirectory;

import query.VerticaSimilarityQuerier;
import query.multiColumn.MultiColTableQuerier;
import query.multiColumn.MultiColVerticaQuerier;
import util.Histogram;
import util.MultiColMatchCase;
import util.Pair;
import util.Similarity;
import util.StemMultiColHistogram;
import util.StemMultiColMap;
import util.fd.tane.FunctionalDependency;
import util.fd.tane.TANEjava;

public class WTTransformerMultiCol {
    public static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
    public static int COVERAGE_THRESHOLD = 2;
    private static final double QUALITY_THRESHOLD = 0.3;
    public boolean fuzzyMatching = false;
    public static double schemaMatchPrior = 0.5;
    public static double schemaMatchWeight = 1;


    public enum ConsolidationMethod {
        NON_FUNCTIONAL,
        FUNCTIONAL,
        MAJORITY
    }

    public ConsolidationMethod consolidation = ConsolidationMethod.FUNCTIONAL;

    /**
     * Those 2 are for progress report
     */
    public String message = null;
    public boolean done = false;
    /*********************************/

    private static final double FD_ERROR = 0.10;

    private static final double SMOOTHING_FACTOR_ALPHA = 0.99;
    public static final MultiColMatchCase QUERY_TABLE_ID = new MultiColMatchCase("query", new int[]{0}, new int[]{1});
    public static final int MAX_ITERS = 100;

    private ConcurrentHashMap<MultiColMatchCase, Double> tableToRating = new ConcurrentHashMap<MultiColMatchCase, Double>();
    private ConcurrentHashMap<MultiColMatchCase, Double> tableToPrior = new ConcurrentHashMap<MultiColMatchCase, Double>();
    private ConcurrentHashMap<MultiColMatchCase, Double> tableToSchemaScore = new ConcurrentHashMap<MultiColMatchCase, Double>();
    private ConcurrentHashMap<MultiColMatchCase, HashSet<Pair<List<String>, List<String>>>> tableToAnswers = new ConcurrentHashMap<>();
    private double initialGoodExampleImportance = 1;
    public static boolean originalOnly = false;

    public enum DATATYPE {
        STRING,
        DATE,
        NUMBER,
        CURRENCY
    }

    private Table inputTable;
    public static boolean toConvergence = true;
    private StemMultiColMap<StemMultiColHistogram> knownExamples = new StemMultiColMap<StemMultiColHistogram>();

    /**
     * Used for fuzzy matching
     */
    private HashMap<List<String>, Collection<List<String>>> exactOriginalExamples = new HashMap<>();

    private StemMultiColMap<StemMultiColHistogram> answerToRating
            = new StemMultiColMap<StemMultiColHistogram>();
    private StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImages
            = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();


    public StemMultiColMap<StemMultiColHistogram> getAllAnswerRatings() {
        return answerToRating;
    }


    //State information for iterations
    private int[] colsFrom;
    private int[] colsTo;
    private MultiColTableQuerier queryBuilder;
    private MultiColTableLoader tableLoader;
    private boolean closedWorld;
    private int inductiveIters;
    private AbstractColumnMatcher[] colMatchersFrom;
    private AbstractColumnMatcher[] colMatchersTo;
    private boolean useNewlyDiscoveredKeys;
    private boolean augment;


    public WTTransformerMultiCol(Table inputTable) {
        this.inputTable = inputTable;
    }

    public static void main(String[] args) {

        int[] colsFrom = new int[]{0};
        int[] colsTo = new int[]{1};

        int topk = 1000;
        int inductiveIters = 1; //1 means no induction

        /**
         * If true, it means that a value for an example different than what's specified is deemed wrong
         */
        boolean closedWorld = false;
        AbstractColumnMatcher[] colMatchersFrom = null;
        AbstractColumnMatcher[] colMatchersTo = null;
        String[] keywords = null;
        boolean augment = false;
        int maxNQueries = -1;
        try {
            Options clop = new Options();
            clop.addOption("qf", "query-file", true, "input (query) file");
            clop.addOption("ac", "anchor-column", true, "anchor column");
            clop.addOption("tc", "target-column", true, "target column");

            OptionGroup tblSrcGrp = new OptionGroup();
            tblSrcGrp.addOption(new Option("fs", "files", true, "directory for a filesystem-based repository"));
            tblSrcGrp.addOption(new Option("db", "database", true, "database name for database-based repository"));
            tblSrcGrp.addOption(new Option("dn", "dresden", true, "directory containing Dresden .json files"));

            clop.addOptionGroup(tblSrcGrp);

            clop.addOption("ix", "index-dir", true, "index directory");

            clop.addOption("topk", true, "How many documents to fetch per lucene query");
            //clop.addOption("fd", true, "Require functional transformation, with error threshold given");

            clop.addOption("i", "inductive", true, "if present, makes inductive iterations, specified by the argument of this option");
            clop.addOption("cw", "closed-world", false, "specifies closed-world semantics");

            Option kwOpt = new Option("kw", "keywords", true, "keywords to look for in the title and context of tables");
            kwOpt.setArgs(15);
            clop.addOption(kwOpt);

            Option mfOpt = new Option("mf", "column-mapper-from", true, "choose column mapper for 'from' column ('st' for stemmer (default), 'kb' <absolute kbpath> for sparql-based, "
                    + "'fb' for Freebase (online), 'oc' for opencalais (cache file is in the classpath))");
            mfOpt.setArgs(2);
            clop.addOption(mfOpt);


            Option mtOpt = new Option("mt", "column-mapper-to", true, "choose column mapper for 'to' column ('st' for stemmer (default), 'kb' <absolute kbpath> for sparql-based, "
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

            if (cl.hasOption("topk")) {
                topk = Integer.parseInt(cl.getOptionValue("topk"));
            }

            if (cl.hasOption("i")) {
                inductiveIters = Integer.parseInt(cl.getOptionValue("i"));
            }

            if (cl.hasOption("cw")) {
                closedWorld = true;
            }
            if (cl.hasOption("ag")) {
                augment = true;
            }

            if (cl.hasOption("kw")) {
                keywords = cl.getOptionValues("kw");
            }

            Table inputTable = new Table(inputTableFile);
            IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)));

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

            WTTransformerMultiCol transformer = new WTTransformerMultiCol(null);
            StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImages = transformer.transform(
                    colsFrom, colsTo, queryBuilder, tableLoader,
                    closedWorld, inductiveIters, colMatchersFrom, colMatchersTo,
                    keywords, augment, false);

            transformer.printResult(keyToImages);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> transform(
            int[] colsFrom, int[] colsTo, MultiColTableQuerier queryBuilder, final MultiColTableLoader tableLoader,
            final boolean closedWorld,
            int inductiveIters, final AbstractColumnMatcher[] colMatchersFrom, final AbstractColumnMatcher[] colMatchersTo,
            String[] keywords, final boolean useNewlyDiscoveredKeys, boolean augment)
            throws IOException, ParseException, SQLException, InterruptedException, ExecutionException {
        this.colsFrom = colsFrom;
        this.colsTo = colsTo;
        this.queryBuilder = queryBuilder;
        this.tableLoader = tableLoader;
        this.closedWorld = closedWorld;
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

                knownExamples.put(k, new StemMultiColHistogram());
                knownExamples.get(k).increment(v, initialGoodExampleImportance);

                exactOriginalExamples.put(k, new HashSet<List<String>>());

                HashSet<MultiColMatchCase> evidence = new HashSet<MultiColMatchCase>();
                evidence.add(QUERY_TABLE_ID);


                keyToImages.get(k).put(v, evidence);

                exactOriginalExamples.get(k).add(v);
                tableToAnswers.get(QUERY_TABLE_ID).add(new Pair<List<String>, List<String>>(k, v));
            }
        }


        return iterate();
    }


    /**
     * FUNCTIONAL
     */
    public StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> recomputeFunctional(StemMultiColMap<StemMultiColHistogram> newKnownExamples)
            throws IOException, ParseException, InterruptedException, ExecutionException, SQLException {
        knownExamples = newKnownExamples;

        return iterate();
    }


    /**
     * @return
     * @throws IOException
     * @throws ParseException
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws SQLException
     */
    private StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> iterate() throws IOException, ParseException,
            InterruptedException, ExecutionException, SQLException {
        if (fuzzyMatching && !(queryBuilder instanceof VerticaSimilarityQuerier)) {
            throw new IllegalStateException("A VerticaSimilarityQuerier must be used"
                    + " when using fuzzy matching");
        }

        if (fuzzyMatching) {
//			((VerticaSimilarityQuerier) queryBuilder).exactKnownExamples = exactOriginalExamples;
            throw new UnsupportedOperationException("Fuzzy matching for multicolumn is not developed yet. Sorry!");
        }

        HashSet<MultiColMatchCase> seenTriples = new HashSet<MultiColMatchCase>();
        int iter = 0;
        StemMultiColMap<StemMultiColHistogram> lastKnownExamples
                = new StemMultiColMap<StemMultiColHistogram>();
        StemMultiColMap<String> foundXs = new StemMultiColMap<String>();

        boolean doQueries = true;

        done = false;
        message = "Initializing";

        answerToRating.putAll(knownExamples);
        while (true) {
            //Clone knownExamples
            for (List<String> k : knownExamples.keySet()) {
                StemMultiColHistogram s = new StemMultiColHistogram();
                for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                    s.increment(v, knownExamples.get(k).getScoreOf(v));
                }
                lastKnownExamples.put(k, s);
            }


            int processedTablesCount = 0;
            StemMultiColMap<String> newlyFoundXs = new StemMultiColMap<String>();


            if (doQueries) {
                message = "Iteration " + (iter + 1) + " - querying for tables";

                ArrayList<MultiColMatchCase> tablesToCheck =
                        queryBuilder.findTables(knownExamples, COVERAGE_THRESHOLD);
                if (tableLoader instanceof BatchTableLoader) {
                    HashSet<String> tableIDs = new HashSet<String>();
                    for (MultiColMatchCase MultiColMatchCase : tablesToCheck) {
                        tableIDs.add(MultiColMatchCase.tableID);
                    }
                    ((BatchTableLoader) tableLoader).loadTables(tableIDs.toArray(new String[0]));
                }
                message = "Iteration " + (iter + 1) + " - checking tables";

                ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREADS);
                ArrayList<TableChecker> tableCheckers = new ArrayList<WTTransformerMultiCol.TableChecker>();

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
                        TableChecker tc = new TableChecker(keyToImages, tableLoader, MultiColMatchCase,
                                colMatchersFrom, colMatchersTo,
                                useNewlyDiscoveredKeys, closedWorld);
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


                        for (List<String> k : partialResult.keySet()) {
                            if (!keyToImages.containsKey(k)) {
                                if (!useNewlyDiscoveredKeys) {
                                    continue;
                                }
                                keyToImages.put(k, new StemMultiColMap<HashSet<MultiColMatchCase>>());
                            }
                            for (List<String> v : partialResult.get(k).keySet()) {
                                if (!keyToImages.get(k).containsKey(v)) {
                                    keyToImages.get(k).put(v, new HashSet<MultiColMatchCase>());
                                }
                                keyToImages.get(k).get(v).addAll(partialResult.get(k).get(v));
                            }

                            if (!foundXs.containsKey(k)) {
                                newlyFoundXs.put(k, "");
                                foundXs.put(k, "");
                            }
                        }
                    }
                }


                double secs = (System.currentTimeMillis() - start) * 1.0 / 1000;
                System.out.println("Finished checking tables, time = " + secs);
            }


            //Maximization: revise table ratings
            tableToRating.put(QUERY_TABLE_ID, 1.0);

            for (MultiColMatchCase MultiColMatchCase : tableToAnswers.keySet()) {

                if (MultiColMatchCase.equals(QUERY_TABLE_ID)) {
                    tableToRating.put(MultiColMatchCase, 1.0);
                    continue;
                }

                double rating = computeRating(MultiColMatchCase);

                tableToRating.put(MultiColMatchCase, rating);
            }


            message = "Iteration " + (iter + 1) + " - adjusting answer scores";

            //Expectation: assess the scores and update examples
            knownExamples = new StemMultiColMap<>();
            answerToRating = new StemMultiColMap<>();


            ArrayList<Future<Pair<List<String>, StemMultiColHistogram>>> futures
                    = new ArrayList<Future<Pair<List<String>, StemMultiColHistogram>>>(keyToImages.size());
            ExecutorService es = Executors.newFixedThreadPool(MAX_THREADS);

            for (final List<String> k : keyToImages.keySet()) {
                if (keyToImages.get(k).size() > 0) {


                    futures.add(es.submit(new Callable<Pair<List<String>, StemMultiColHistogram>>() {
                        @Override
                        public Pair<List<String>, StemMultiColHistogram> call()
                                throws Exception {

                            StemMultiColHistogram distrib = computeAnswerScores(keyToImages.get(k));
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

                StemMultiColHistogram s = new StemMultiColHistogram();
                s.increment(bestEntry.key, bestEntry.value);
                knownExamples.put(k, s);

                answerToRating.put(k, distrib);
            }
            int tid = 1;

            //Check for convergence
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

                //For the ohter side
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
                //new iteration did not produce significant improvement
                if (errorSum < 1e-5) {
                    break;
                }
            }


            System.out.print("" + processedTablesCount + "T");
            System.out.print("-");
            System.out.println();
            System.out.println();

            iter++;

            if (iter >= MAX_ITERS) {
                break;
            }

            if (iter >= inductiveIters || newlyFoundXs.size() == 0) {
                doQueries = false;
                //System.out.println("Can't cover more X's...no more queries");
                message = "Can't cover more X's...no more queries to the corpus";
                if (!toConvergence) {
                    break;
                }
            }

        }// END WHILE TRUE

        done = true;
        if (augment) {
            return keyToImages;
        } else {
            //Remove spurious ones
            StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImagesFinal
                    = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();

            for (Tuple t : inputTable.getTuples()) {
                List<String> k = t.getValuesOfCells(colsFrom);
                keyToImagesFinal.put(k, new StemMultiColMap<HashSet<MultiColMatchCase>>());

                if (knownExamples.containsKey(k)) {
                    if (knownExamples.get(k).getCountsUnsorted().size() > 0) {
                        List<String> v = knownExamples.get(k).getCountsSorted().get(0).key;
                        keyToImagesFinal.get(k).put(v, keyToImages.get(k).get(v));
                    }
                }


            }
            return keyToImagesFinal;
        }
    }

    //FIXME: Update table rating computation
    private double computeRating(MultiColMatchCase MultiColMatchCase) {
        double prior = tableToPrior.get(MultiColMatchCase);

        double goodSum = 0;
        double totalSum = 0;

        HashSet<List<String>> coveredKeys = new HashSet<>();

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
                }
            }

        }

        double unseenWeightSum = 0;
        for (List<String> k : knownExamples.keySet()) {
            if (!coveredKeys.contains(k)) {
                unseenWeightSum += knownExamples.get(k).getTotalCount();
//				unseenWeightSum++;
            }
        }
        double instanceBasedRating = (goodSum + prior * unseenWeightSum) * SMOOTHING_FACTOR_ALPHA / (totalSum + unseenWeightSum);
        double schemaScore = tableToSchemaScore.get(MultiColMatchCase);

        return (instanceBasedRating + schemaScore * schemaMatchWeight) / (1 + schemaMatchWeight);
    }


    private double computeSchemaMatch(String[] h1, String[] h2) {
        String[] headers1 = inputTable.getColumnMapping().getColumnNames(colsFrom);
        String[] headers2 = inputTable.getColumnMapping().getColumnNames(colsTo);

        double s1 = Similarity.similarity(StemMultiColMap.stemList(headers1), StemMultiColMap.stemList(h1));
        double s2 = Similarity.similarity(StemMultiColMap.stemList(headers2), StemMultiColMap.stemList(h2));

        if (s1 < 0.5) {
            s1 = 0;
        }
        if (s2 < 0.5) {
            s2 = 0;
        }
        return (s1 + s2) / 2;

    }


    public StemMultiColMap<Pair<List<String>, Double>> transformFunctional(
            final int[] colsFrom, int[] colsTo, MultiColTableQuerier queryBuilder,
            final MultiColTableLoader tableLoader,
            int keysPerQuery,
            final boolean closedWorld, int inductiveIters, final AbstractColumnMatcher[] colMatchersFrom,
            final AbstractColumnMatcher[] colMatchersTo, String[] keywords, final boolean useNewlyDiscoveredKeys,
            boolean augment)
            throws IOException, ParseException, SQLException, InterruptedException, ExecutionException {
        StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> r = transform(colsFrom, colsTo, queryBuilder, tableLoader,
                closedWorld, inductiveIters, colMatchersFrom,
                colMatchersTo, keywords, useNewlyDiscoveredKeys,
                augment);

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
     * @return (good, bad)
     */
    public Pair<List<Double>, List<Double>> checkKnownTupleMapping(Table t, int[] anchorCols, int[] depCols,
                                                                   StemMultiColMap<StemMultiColHistogram> knownExamples, boolean closedWorld) {
        List<Double> good = new ArrayList<Double>();
        List<Double> bad = new ArrayList<Double>();

        if (!fuzzyMatching) {
            StemMultiColMap<StemMultiColHistogram> tableAsTree = createMapFromTable(t, anchorCols, depCols);


            for (List<String> k : tableAsTree.keySet()) {
                if (knownExamples.containsKey(k)) {
                    for (List<String> v : tableAsTree.get(k).getCountsUnsorted().keySet()) {
                        if (knownExamples.get(k).containsKey(v)) {
                            good.add(knownExamples.get(k).getScoreOf(v));
                        } else {
                            bad.add(1.0);
                        }
                    }
                }

            }
        } else //With fuzzy matching
        {
            throw new UnsupportedOperationException("Fuzzy matching for multicolumn is not developed yet. Sorry!");

        }


        return new Pair<List<Double>, List<Double>>(good, bad);

    }


    /**
     * @param goods
     * @param bads
     * @param closedWorld
     * @return
     */
    public static boolean assessTupleMapping(List<Double> goods, List<Double> bads, boolean closedWorld) {
        double good = 0;
        double bad = 0;

        for (int i = 0; i < goods.size(); i++) {
            good += goods.get(i);
        }
        for (int i = 0; i < bads.size(); i++) {
            bad += bads.get(i);
        }

        int goodCount = goods.size();

        if (closedWorld) {
            return goodCount >= COVERAGE_THRESHOLD && (good * 1.0 / (good + bad)) >= QUALITY_THRESHOLD;
        } else {
            return goodCount >= COVERAGE_THRESHOLD;
        }
    }


    public static StemMultiColMap<StemMultiColHistogram>
    createMapFromTable(Table table, int[] colsFrom, int[] colsTo) {
        StemMultiColMap<StemMultiColHistogram> tableAsTree
                = new StemMultiColMap<StemMultiColHistogram>();

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

    public static HashMap<List<String>, Histogram<List<String>>>
    createExactMapFromTable(Table table, int[] colsFrom, int[] colsTo) {
        HashMap<List<String>, Histogram<List<String>>> tableAsTree
                = new HashMap<>();

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
    private static ArrayList<FunctionalDependency> findFDsWithColAsLHS(Table table, int anchorColIdx) throws Exception {
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
    private static ArrayList<FunctionalDependency> findFDsWithColInLHS(Table table, int anchorColIdx) throws Exception {
        ArrayList<FunctionalDependency> allFDs = findFDs(table);

        ArrayList<FunctionalDependency> fdsWithColInLHS = new ArrayList<>();

        for (FunctionalDependency fd : allFDs) {
            if (fd.getX().contains(anchorColIdx)) {
                fdsWithColInLHS.add(fd);
            }
        }

        return fdsWithColInLHS;
    }


    public static DATATYPE findTypeOfList(Collection<String> values) {
        boolean allNulls = true;
        for (String v : values) {
            if (v != null) {
                allNulls = false;
                break;
            }
        }
        if (allNulls) {
            return null;
        }

        if (allMatchRegex(values, "^(\\s|&nbsp;)*-?(\\s|&nbsp;)*([0-9]+(\\.[0-9]+)?|[0]*\\.[0-9]+)((\\s|&nbsp;)?%)?(\\s|&nbsp;)*$")) {
            return DATATYPE.NUMBER;
        } else if (allMatchRegex(values, "^(\\s|&nbsp;)*[0-3]?[0-9](-|/|.)[0-3]?[0-9](-|/|.)([0-9]{2})?[0-9]{2}(\\s|&nbsp;)*$")) {
            return DATATYPE.DATE;
        } else if (allMatchRegex(values, "^(\\s|&nbsp;)*(\\$|\\u20ac|EUR|USD|CAD|C\\$)( )?\\d+ | \\d+( )?(\\$|\\u20ac|EUR|USD|CAD|C\\$)(\\s|&nbsp;)*$")) {
            return DATATYPE.CURRENCY;
        }
        return DATATYPE.STRING;
    }


    public static boolean allMatchRegex(Collection<String> values, String regex) {
        for (String v : values) {
            if (v != null && !v.matches(regex)) {
                return false;
            }
        }

        return true;
    }


    /**
     * Computes the score of a value given the evidence from the specified tableIDs
     *
     * @param answers
     * @return probability distribution of the values (null = unknown...)
     */
    public StemMultiColHistogram computeAnswerScores(HashMap<List<String>, HashSet<MultiColMatchCase>> answers) {

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

            //Normalize
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

            //Normalize
            StemMultiColHistogram normalizedDistrib = new StemMultiColHistogram();
            for (List<String> v : answers.keySet()) {
                if ((v == null && bestAnswer == null) || (v != null && v.equals(bestAnswer))) {
                    normalizedDistrib.increment(v, 0.5 + (answers.get(bestAnswer).size() * 0.5 / tableToAnswers.size()));
                } else {
                    normalizedDistrib.increment(v, 0.0);
                }
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
        public HashMap<MultiColMatchCase, Double> tableToPriorPartial = new HashMap<MultiColMatchCase, Double>();
        public HashMap<MultiColMatchCase, Double> tableToSchemaPriorPartial = new HashMap<MultiColMatchCase, Double>();
        public HashMap<MultiColMatchCase, HashSet<Pair<List<String>, List<String>>>> tableToAnswersPartial
                = new HashMap<MultiColMatchCase, HashSet<Pair<List<String>, List<String>>>>();
        public StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> result
                = new StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>>();


        public TableChecker(StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImages,
                            MultiColTableLoader tableLoader, MultiColMatchCase multiColMatchCase,
                            AbstractColumnMatcher[] colMatchersFrom,
                            AbstractColumnMatcher[] colMatchersTo,
                            boolean useNewlyDiscoveredKeys, boolean closedWorld) {
            super();
            this.keyToImages = keyToImages;
            this.tableLoader = tableLoader;
            this.multiColMatchCase = multiColMatchCase;
            this.colMatchersFrom = colMatchersFrom;
            this.colMatchersTo = colMatchersTo;
            this.useNewlyDiscoveredKeys = useNewlyDiscoveredKeys;
            this.closedWorld = closedWorld;
        }


        @Override
        public void run() {
            try {
                String tableID = multiColMatchCase.tableID;
                int[] colsF = multiColMatchCase.col1;
                int[] colsT = multiColMatchCase.col2;

                Table webTable = tableLoader.loadTable(multiColMatchCase);

                Pair<List<Double>, List<Double>> checkResult = checkKnownTupleMapping(
                        webTable, colsF, colsT, knownExamples, closedWorld);


                List<Double> goods = checkResult.key;
                List<Double> bads = checkResult.value;


                if (assessTupleMapping(goods, bads, closedWorld)) {
                    //Transformation found

                    for (Tuple tup : webTable.getTuples()) {
                        List<String> k = tup.getValuesOfCells(colsF);
                        List<String> v = tup.getValuesOfCells(colsT); //the transformed value

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
                                tableToAnswersPartial.put(multiColMatchCase, new HashSet<Pair<List<String>, List<String>>>());
                            }
                            tableToAnswersPartial.get(multiColMatchCase).add(new Pair<List<String>, List<String>>(k, v));

                            result.get(k).get(v).add(multiColMatchCase);
                        }
                    }
                    tableToPriorPartial.put(multiColMatchCase, (double) webTable.confidence);

                    double schemaMatchScore;
                    if (webTable.hasHeader()) {
                        String[] h1 = webTable.getColumnMapping().getColumnNames(multiColMatchCase.col1);
                        String[] h2 = webTable.getColumnMapping().getColumnNames(multiColMatchCase.col2);

                        schemaMatchScore = computeSchemaMatch(h1, h2);
                    } else {
                        schemaMatchScore = schemaMatchPrior;
                    }
                    tableToSchemaPriorPartial.put(multiColMatchCase, schemaMatchScore);
                } else //TODO: indirect coverage?
                {

                }

                //System.out.println("table " + tableID + " checked --- " + webTable.getNumRows() + " rows");
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
        tableToAnswers.get(QUERY_TABLE_ID).add(new Pair<List<String>, List<String>>(k, v));


        if (!keyToImages.containsKey(k)) {
            keyToImages.put(k, new StemMultiColMap<HashSet<MultiColMatchCase>>());
        }
        if (!keyToImages.get(k).containsKey(v)) {
            keyToImages.get(k).put(v, new HashSet<MultiColMatchCase>());
        }
        keyToImages.get(k).get(v).add(QUERY_TABLE_ID);


    }


    public void setBadExample(List<String> k, List<String> v) {

        if (!knownExamples.containsKey(k)) {
            knownExamples.put(k, new StemMultiColHistogram());
        }
        knownExamples.get(k).setScoreOf(v, -initialGoodExampleImportance);
        tableToAnswers.get(QUERY_TABLE_ID).add(new Pair<List<String>, List<String>>(k, v));

        if (!keyToImages.containsKey(k)) {
            keyToImages.put(k, new StemMultiColMap<HashSet<MultiColMatchCase>>());
        }
        if (!keyToImages.get(k).containsKey(v)) {
            keyToImages.get(k).put(v, new HashSet<MultiColMatchCase>());
        }
        keyToImages.get(k).get(v).add(QUERY_TABLE_ID);

        throw new UnsupportedOperationException("STILL NEED TO TUNE PUNISHMENT");
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
            VerticaBatchTableLoader.setPrior(tableID, prior + (1 - prior) * tableToCount.getScoreOf(tableID) * SMOOTHING_FACTOR_ALPHA / count);
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
    public MultiColMatchCase getBestTableForAnswer(String k, String v) {
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