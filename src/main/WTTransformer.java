package main;

import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.FileNotFoundException;
import java.io.IOException;
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

import model.Table;
import model.TableLoader;
import model.Tuple;
import model.VerticaBatchTableLoader;
import model.columnMapping.AbstractColumnMatcher;
import org.apache.lucene.queryparser.classic.ParseException;

import query.TableQuerier;
import query.VerticaSimilarityQuerier;
import util.Histogram;
import util.Pair;
import util.Similarity;
import util.StemHistogram;
import util.StemMap;
import util.MatchCase;
import util.fd.tane.FunctionalDependency;
import util.fd.tane.TANEjava;

public class WTTransformer {
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


    //private static final String ROOT_TABLE_CODE = "root";
    private static final double FD_ERROR = 0.10;

    private static final double SMOOTHING_FACTOR_ALPHA = 0.99;
    public static final MatchCase QUERY_TABLE_ID = new MatchCase("query", 0, 1);
    public static final int MAX_ITERS = 100;

    private ConcurrentHashMap<MatchCase, Double> tableToRating = new ConcurrentHashMap<MatchCase, Double>();
    private ConcurrentHashMap<MatchCase, Double> tableToPrior = new ConcurrentHashMap<MatchCase, Double>();
    private ConcurrentHashMap<MatchCase, Double> tableToSchemaScore = new ConcurrentHashMap<MatchCase, Double>();
    private ConcurrentHashMap<MatchCase, HashSet<Pair<String, String>>> tableToAnswers = new ConcurrentHashMap<>();
    private double initialGoodExampleImportance = 1;
    public static boolean originalOnly = false;


    private static int maxLength = 200;

    public static boolean verbose = true;

    public enum DATATYPE {
        STRING,
        DATE,
        NUMBER,
        CURRENCY
    }


    private Table inputTable = null;
    public static TIntObjectHashMap<String> idToExternalId = new TIntObjectHashMap<>();
    public static boolean toConvergence = true;
    private StemMap<StemHistogram> knownExamples = new StemMap<StemHistogram>();

    /**
     * Used for fuzzy matching
     */
    private HashMap<String, Collection<String>> exactOriginalExamples = new HashMap<String, Collection<String>>();

    private StemMap<StemHistogram> answerToRating = new StemMap<StemHistogram>();
    private StemMap<StemMap<HashSet<MatchCase>>> keyToImages = new StemMap<StemMap<HashSet<MatchCase>>>();


    public StemMap<StemHistogram> getAllAnswerRatings() {
        return answerToRating;
    }


    //State information for iterations
    private int colFrom;
    private int colTo;
    private TableQuerier queryBuilder;
    private TableLoader tableLoader;
    private String[] keywords;
    private boolean closedWorld;
    private int inductiveIters;
    private AbstractColumnMatcher colMatcherFrom;
    private AbstractColumnMatcher colMatcherTo;
    private boolean useNewlyDiscoveredKeys;
    private boolean augment;
    private Table knownRows;


    public WTTransformer(Table inputTable) {
        this.inputTable = inputTable;
    }

    public StemMap<StemMap<HashSet<MatchCase>>> transform(
            int colFrom, int colTo, TableQuerier queryBuilder, final TableLoader tableLoader,
            final boolean closedWorld,
            int inductiveIters, final AbstractColumnMatcher colMatcherFrom, final AbstractColumnMatcher colMatcherTo,
            String[] keywords, final boolean useNewlyDiscoveredKeys, boolean augment)
            throws IOException, ParseException, SQLException, InterruptedException, ExecutionException {
        this.colFrom = colFrom;
        this.colTo = colTo;
        this.queryBuilder = queryBuilder;
        this.tableLoader = tableLoader;
        this.closedWorld = closedWorld;
        this.inductiveIters = inductiveIters;
        this.colMatcherFrom = colMatcherFrom;
        this.colMatcherTo = colMatcherTo;
        this.keywords = keywords;
        this.useNewlyDiscoveredKeys = useNewlyDiscoveredKeys;
        this.augment = augment;
        //separate into known and unknown
        this.knownRows = new Table(inputTable, false);
        for (Tuple tuple : inputTable.getTuples()) {
            if (tuple.getCell(colTo).getValue() != null) {
                knownRows.addTuple(tuple);
            }
        }


        for (Tuple t : inputTable.getTuples()) {
            String v = t.getCell(colFrom).getValue();

            keyToImages.put(v, new StemMap<HashSet<MatchCase>>());
        }


        for (Tuple t : knownRows.getTuples()) {
            String k = t.getCell(colFrom).getValue();

            knownExamples.put(k, new StemHistogram());
            exactOriginalExamples.put(k, new HashSet<String>());
        }


        tableToAnswers.put(QUERY_TABLE_ID, new HashSet<Pair<String, String>>());

        for (Tuple t : knownRows.getTuples()) {
            HashSet<MatchCase> evidence = new HashSet<MatchCase>();
            evidence.add(QUERY_TABLE_ID);


            String k = t.getCell(colFrom).getValue();
            String v = t.getCell(colTo).getValue();


            keyToImages.get(k).put(v, evidence);

            knownExamples.get(k).increment(v, initialGoodExampleImportance);
            exactOriginalExamples.get(k).add(v);
            tableToAnswers.get(QUERY_TABLE_ID).add(new Pair<String, String>(k, v));
        }
        return iterate();
    }


    /**
     * FUNCTIONAL
     *
     * @param newKnownExamples
     * @throws SQLException
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws ParseException
     * @throws IOException
     * @throws FileNotFoundException
     */
    public StemMap<StemMap<HashSet<MatchCase>>> recomputeFunctional(StemMap<StemHistogram> newKnownExamples) throws FileNotFoundException, IOException, ParseException, InterruptedException, ExecutionException, SQLException {
        knownExamples = newKnownExamples;
        //Must update the exactOriginalExamples as well
        throw new UnsupportedOperationException("Must update the exactOriginalExamples as well");
        //return iterate();
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
    private StemMap<StemMap<HashSet<MatchCase>>> iterate() throws IOException, ParseException,
            InterruptedException, ExecutionException, SQLException,
            FileNotFoundException {
        if (fuzzyMatching && !(queryBuilder instanceof VerticaSimilarityQuerier)) {
            throw new IllegalStateException("A VerticaSimilarityQuerier must be used"
                    + " when using fuzzy matching");
        }

        if (fuzzyMatching) {
            ((VerticaSimilarityQuerier) queryBuilder).exactKnownExamples = exactOriginalExamples;
        }

        HashSet<MatchCase> seenTriples = new HashSet<MatchCase>();
        int iter = 0;
        StemMap<StemHistogram> lastKnownExamples = new StemMap<StemHistogram>();
        StemMap<String> foundXs = new StemMap<String>();

        boolean doQueries = true;

        done = false;
        message = "Initializing";

        answerToRating.putAll(knownExamples);
        while (true) {
            //Clone knownExamples
            for (String k : knownExamples.keySet()) {
                StemHistogram s = new StemHistogram();
                for (String v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                    s.increment(v, knownExamples.get(k).getScoreOf(v));
                }
                lastKnownExamples.put(k, s);
            }


            int processedTablesCount = 0;
            StemMap<String> newlyFoundXs = new StemMap<String>();


            if (doQueries) {
                message = "Iteration " + (iter + 1) + " - querying for tables (step 1/4)";

                ArrayList<MatchCase> tablesToCheck =
                        queryBuilder.findTables(keyToImages, knownExamples, COVERAGE_THRESHOLD);
                if (tableLoader instanceof VerticaBatchTableLoader) {
                    HashSet<String> tableIDs = new HashSet<String>();
                    for (MatchCase matchCase : tablesToCheck) {
                        tableIDs.add(matchCase.tableID);
                    }
                    ((VerticaBatchTableLoader) tableLoader).loadTables(tableIDs.toArray(new String[0]));
                }
                message = "Iteration " + (iter + 1) + " - checking tables (step 2/4)";

                ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREADS);
                ArrayList<TableChecker> tableCheckers = new ArrayList<WTTransformer.TableChecker>();

                long start = System.currentTimeMillis();
                for (final MatchCase matchCase : tablesToCheck) {
                    if (seenTriples.contains(matchCase)) {
                        continue;
                    } else {
                        if (originalOnly) {
                            Table webTable = tableLoader.loadTable(matchCase);
                            if (webTable.source != 1) {
                                continue;
                            }
                        }

                        seenTriples.add(matchCase);
                        TableChecker tc = new TableChecker(keyToImages, tableLoader, matchCase,
                                colMatcherFrom, colMatcherTo,
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

                message = "Iteration " + (iter + 1) + " - consolidating results (step 3/4)";


                for (TableChecker tc : tableCheckers) {
                    if (tc.tableToPriorPartial.size() > 0) {
                        StemMap<StemMap<HashSet<MatchCase>>> partialResult = tc.result;
                        tableToPrior.putAll(tc.tableToPriorPartial);
                        tableToAnswers.putAll(tc.tableToAnswersPartial);
                        tableToSchemaScore.putAll(tc.tableToSchemaPriorPartial);

//						MatchCase matchCase = tc.getMatchCase();
//						tableToRating.put(matchCase, computeRating(matchCase));


                        for (String k : partialResult.keySet()) {
                            if (!keyToImages.containsKey(k)) {
                                if (!useNewlyDiscoveredKeys) {
                                    continue;
                                }
                                keyToImages.put(k, new StemMap<HashSet<MatchCase>>());
                            }
                            for (String v : partialResult.get(k).keySet()) {
                                if (!keyToImages.get(k).containsKey(v)) {
                                    keyToImages.get(k).put(v, new HashSet<MatchCase>());
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

            for (MatchCase matchCase : tableToAnswers.keySet()) {

                if (matchCase.equals(QUERY_TABLE_ID)) {
                    tableToRating.put(matchCase, 1.0);
                    continue;
                }


                double rating = computeRating(matchCase);

                tableToRating.put(matchCase, rating);
            }


            message = "Iteration " + (iter + 1) + " - adjusting answer scores (step 4/4)";

            //Expectation: assess the scores and update examples
            knownExamples = new StemMap<>();
            answerToRating = new StemMap<>();


            ArrayList<Future<Pair<String, StemHistogram>>> futures = new ArrayList<Future<Pair<String, StemHistogram>>>(keyToImages.size());
            ExecutorService es = Executors.newFixedThreadPool(MAX_THREADS);

            for (final String k : keyToImages.keySet()) {
                if (keyToImages.get(k).size() > 0) {


                    futures.add(es.submit(new Callable<Pair<String, StemHistogram>>() {
                        @Override
                        public Pair<String, StemHistogram> call()
                                throws Exception {
                            StemHistogram distrib = computeAnswerScores(keyToImages.get(k));
                            return new Pair<String, StemHistogram>(k, distrib);
                        }
                    }));
                }
            }

            es.shutdown();
            while (!es.awaitTermination(1, TimeUnit.HOURS)) {

            }


            for (Future<Pair<String, StemHistogram>> future : futures) {
                String k = future.get().key;
                StemHistogram distrib = future.get().value;
                double scoreOfNone = 1 - distrib.getTotalCount();
                Pair<String, Double> bestEntry = distrib.getCountsSorted().get(0);
                double maxScore = bestEntry.value;

//				if(maxScore > scoreOfNone)
//				{
                StemHistogram s = new StemHistogram();
                s.increment(bestEntry.key, bestEntry.value);
                knownExamples.put(k, s);
                //System.out.println(k + " -> " + bestEntry.key + " " + maxScore);

//					knownExamples.put(k,  distrib);
//				}
                answerToRating.put(k, distrib);
            }


            int tid = 1;
            //knownRows = new Table(knownRows, false);
            knownRows.retainTuple(new HashSet<Tuple>()); //retain none = delete all
            for (String k : keyToImages.keySet()) {
                if (keyToImages.get(k).size() > 0) {
                    for (String v : keyToImages.get(k).keySet()) {
                        String[] values = new String[knownRows.getNumCols()];
                        values[colFrom] = k;
                        values[colTo] = v;
                        Tuple t = new Tuple(values, knownRows.getColumnMapping(), tid++);
                        knownRows.addTuple(t);

                    }
                }
            }


            //Check for convergence
            if (!doQueries) {
                double errorSum = 0;

                for (String k : knownExamples.keySet()) {
                    for (String v : knownExamples.get(k).getCountsUnsorted().keySet()) {
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
                for (String k : lastKnownExamples.keySet()) {
                    for (String v : lastKnownExamples.get(k).getCountsUnsorted().keySet()) {
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


            System.out.print("" + processedTablesCount + "T");
            System.out.print("-");
            //Pair<Double, Double> pr = GeneralTests.precisionRecall(keyToImages, knownExamples, GeneralTests.groundTruth);
            //System.out.println("iter" + iter + ", " + pr.key + ", " + pr.value);
            System.out.println();
            System.out.println();

            iter++;

            if (iter >= MAX_ITERS) {
                break;
            }


            if (iter >= inductiveIters || newlyFoundXs.size() == 0) {
                doQueries = false;
                if (newlyFoundXs.size() == 0) {
                    System.out.println("Can't cover more X's...no more queries");
                    message = "Can't cover more X's...no more queries to the corpus";
                } else {
                    System.out.println("Reached the maximum number of allowed querying (inductive) iterations");
                    message = "Reached the maximum number of allowed querying (inductive) iterations";
                }
                if (!toConvergence) {
                    break;
                }
            }


        }

        done = true;
        if (augment) {
            return keyToImages;
        } else {
            //Remove spurious ones
            StemMap<StemMap<HashSet<MatchCase>>> keyToImagesFinal = new StemMap<StemMap<HashSet<MatchCase>>>();

            for (Tuple t : inputTable.getTuples()) {
                String k = t.getCell(colFrom).getValue();
                keyToImagesFinal.put(k, new StemMap<HashSet<MatchCase>>());

                if (knownExamples.containsKey(k)) {
                    if (knownExamples.get(k).getCountsUnsorted().size() > 0) {
                        String v = knownExamples.get(k).getCountsSorted().get(0).key;
                        keyToImagesFinal.get(k).put(v, keyToImages.get(k).get(v));
                    }
                }


            }
            return keyToImagesFinal;
        }
    }


    private double computeRating(MatchCase matchCase) {
        double prior = tableToPrior.get(matchCase);


        double goodSum = 0;
        double totalSum = 0;

        HashSet<String> coveredKeys = new HashSet<String>();

        for (Pair<String, String> entry : tableToAnswers.get(matchCase)) {
            String k = entry.key;
            String v = entry.value;

            coveredKeys.add(k);


            if (knownExamples.containsKey(k)) {
                if (knownExamples.get(k).getCountsUnsorted().containsKey(v)) {
                    goodSum += knownExamples.get(k).getScoreOf(v);
                    totalSum += knownExamples.get(k).getScoreOf(v);
                } else {
                    totalSum += knownExamples.get(k).getTotalCount();
//					totalSum += knownExamples.get(k).getCountsSorted().get(0).value;
                }
            }

        }

        double unseenWeightSum = 0;
        for (String k : knownExamples.keySet()) {
            if (!coveredKeys.contains(k)) {
                unseenWeightSum += knownExamples.get(k).getTotalCount();
//				unseenWeightSum++;
            }
        }
        double instanceBasedRating = (goodSum + prior * unseenWeightSum) * SMOOTHING_FACTOR_ALPHA / (totalSum + unseenWeightSum);

        //return instanceBasedRating;
        double schemaScore = tableToSchemaScore.get(matchCase);


        double rating = (instanceBasedRating + schemaScore * schemaMatchWeight) / (1 + schemaMatchWeight);
        return rating;
    }


    private double computeSchemaMatch(String h1, String h2) {
        String header1 = inputTable.getColumnMapping().getColumnNames()[colFrom];
        String header2 = inputTable.getColumnMapping().getColumnNames()[colTo];

        double s1 = Similarity.similarity(StemMap.getStem(header1), StemMap.getStem(h1));
        double s2 = Similarity.similarity(StemMap.getStem(header2), StemMap.getStem(h2));

//		return Math.max(schemaMatchPrior, (s1 + s2) / 2);

        if (s1 < 0.5) {
            s1 = 0;
        }
        if (s2 < 0.5) {
            s2 = 0;
        }
        return (s1 + s2) / 2;

    }


    public StemMap<Pair<String, Double>> transformFunctional(
            final int colFrom, int colTo, TableQuerier queryBuilder,
            final TableLoader tableLoader,
            int keysPerQuery,
            final boolean closedWorld, int inductiveIters, final AbstractColumnMatcher colMatcherFrom,
            final AbstractColumnMatcher colMatcherTo, String[] keywords, final boolean useNewlyDiscoveredKeys,
            boolean augment)
            throws IOException, ParseException, SQLException, InterruptedException, ExecutionException {
        StemMap<StemMap<HashSet<MatchCase>>> r = transform(colFrom, colTo, queryBuilder, tableLoader,
                closedWorld, inductiveIters, colMatcherFrom,
                colMatcherTo, keywords, useNewlyDiscoveredKeys,
                augment);

        StemMap<Pair<String, Double>> result = new StemMap<Pair<String, Double>>();

        for (String k : knownExamples.keySet()) {
            result.put(k, knownExamples.get(k).getCountsSorted().get(0));
        }

        for (String k : r.keySet()) {
            if (!result.containsKey(k)) {
                result.put(k, new Pair<String, Double>(null, 0.0));
            }
        }

        return result;
    }


    public void printResult(StemMap<StemMap<HashSet<MatchCase>>> keyToImages) {
        for (String k : keyToImages.keySet()) {
            System.out.println(k);

            StemMap<HashSet<MatchCase>> imageVals = keyToImages.get(k);
            for (String v : imageVals.keySet()) {
                System.out.println("--" + v);
                System.out.println("\t" + keyToImages.get(k).get(v));
            }
            System.out.println("================================================");
        }
    }


    public Pair<List<Double>, List<Double>> checkKnownTupleMapping(Table t, int anchorCol, int depCol,
                                                                   StemMap<StemHistogram> knownExamples, boolean closedWorld,
                                                                   AbstractColumnMatcher colMatcherFrom, AbstractColumnMatcher colMatcherTo) {
        List<Double> good = new ArrayList<Double>();
        List<Double> bad = new ArrayList<Double>();

        if (!fuzzyMatching) {
            StemMap<StemHistogram> tableAsTree = createMapFromTable(t, anchorCol, depCol);


            for (String k : tableAsTree.keySet()) {
                if (knownExamples.containsKey(k)) {
                    for (String v : tableAsTree.get(k).getCountsUnsorted().keySet()) {
                        if (knownExamples.get(k).containsKey(v)) {
                            good.add(knownExamples.get(k).getScoreOf(v));
                        } else {
                            //bad.add(knownExamples.get(k).getCountsSorted().get(0).value);
                            bad.add(1.0);
                        }
                    }
                }

            }
        } else //With fuzzy matching
        {
            if (!(queryBuilder instanceof VerticaSimilarityQuerier)) {
                throw new IllegalStateException("VerticaSimilarityQuerier must be used with fuzzy matching.");
            }

            HashMap<String, Histogram<String>> tableAsTree =
                    createExactMapFromTable(t, anchorCol, depCol);

            HashMap<String, Collection<String>> keyToSimilarKeyFormsTokenized
                    = ((VerticaSimilarityQuerier) queryBuilder).keyToSimilarKeyForms;
            HashMap<String, Collection<String>> valToSimilarValFormsTokenized
                    = ((VerticaSimilarityQuerier) queryBuilder).valToSimilarValForms;

            for (String k : tableAsTree.keySet()) {
                if (knownExamples.containsKey(k)) {
                    for (String v : tableAsTree.get(k).getCountsUnsorted().keySet()) {
                        if (knownExamples.get(k).containsKey(v)) {
                            good.add(knownExamples.get(k).getScoreOf(v));
                        } else {
                            String standardKeyForm = null;

                            for (String standardKey : keyToSimilarKeyFormsTokenized.keySet()) {
                                if (keyToSimilarKeyFormsTokenized.get(standardKey).contains(k)) {
                                    standardKeyForm = standardKey;
                                    break;
                                }
                            }

                            if (standardKeyForm != null) {
                                String standardValForm = null;

                                for (String standardV : exactOriginalExamples.get(standardKeyForm)) {
                                    if (valToSimilarValFormsTokenized.get(standardV).contains(v)) {
                                        standardValForm = standardV;
                                        break;
                                    }
                                }

                                if (standardValForm != null) {
                                    //TODO: original...add distance or 1.0?
                                    good.add(Similarity.similarity(v, standardValForm));
                                } else {
                                    //bad.add(knownExamples.get(k).getCountsSorted().get(0).value);
                                    bad.add(1.0);
                                }

                            }
                        }
                    }
                } else //check if original and within distance
                {
                    String standardKeyForm = null;

                    for (String standardKey : keyToSimilarKeyFormsTokenized.keySet()) {
                        if (keyToSimilarKeyFormsTokenized.get(standardKey).contains(k)) {
                            standardKeyForm = standardKey;
                            break;
                        }
                    }

                    if (standardKeyForm != null) {
                        for (String v : tableAsTree.get(k).getCountsUnsorted().keySet()) {
                            String standardValForm = null;

                            for (String standardV : exactOriginalExamples.get(standardKeyForm)) {
                                if (valToSimilarValFormsTokenized.get(standardV).contains(v)) {
                                    standardValForm = standardV;
                                    break;
                                }
                            }

                            if (standardValForm != null) {
                                //TODO: original...add distance or 1.0?
                                good.add(Similarity.similarity(v, standardValForm));
                            } else {
                                //bad.add(knownExamples.get(k).getCountsSorted().get(0).value);
                                bad.add(1.0);
                            }
                        }
                    }
                }
            }
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


    public static StemMap<StemHistogram> createMapFromTable(Table table, int col1, int col2) {
        StemMap<StemHistogram> tableAsTree = new StemMap<StemHistogram>();

        for (Tuple tuple : table.getTuples()) {
            String k = tuple.getCell(col1).getValue();
            if (!tableAsTree.containsKey(k)) {
                tableAsTree.put(k, new StemHistogram());
            }

            String v = tuple.getCell(col2).getValue();
            tableAsTree.get(k).increment(v);
        }

        return tableAsTree;
    }

    public static HashMap<String, Histogram<String>> createExactMapFromTable(Table table, int col1, int col2) {
        HashMap<String, Histogram<String>> tableAsTree = new HashMap<String, Histogram<String>>();

        for (Tuple tuple : table.getTuples()) {
            String k = tuple.getCell(col1).getValue();
            if (!tableAsTree.containsKey(k)) {
                tableAsTree.put(k, new Histogram<String>());
            }

            String v = tuple.getCell(col2).getValue();
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
    public StemHistogram computeAnswerScores(HashMap<String, HashSet<MatchCase>> answers) {

        if (consolidation == ConsolidationMethod.FUNCTIONAL) {
            StemMap<Double> distrib = new StemMap<Double>();
            double scoreOfNone = 1.0;

            HashSet<MatchCase> allTables = new HashSet<MatchCase>();

            for (String v : answers.keySet()) {
                distrib.put(v, 1.0);
                allTables.addAll(answers.get(v));
            }


            for (MatchCase matchCase : allTables) {

                double tableRating = tableToRating.get(matchCase);
                scoreOfNone *= (1 - tableRating);
                for (String v : answers.keySet()) {
                    double s = distrib.get(v);

                    if (answers.get(v).contains(matchCase)) {
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

            StemHistogram normalizedDistrib = new StemHistogram();
            for (String v : distrib.keySet()) {
                normalizedDistrib.increment(v, distrib.get(v) / sum);
            }

            return normalizedDistrib;
        } else if (consolidation == ConsolidationMethod.MAJORITY) {
            String bestAnswer = null;
            int bestScore = -1;

            for (String answer : answers.keySet()) {
                int score = answers.get(answer).size();
                if (score > bestScore) {
                    bestAnswer = answer;
                    bestScore = score;
                }
            }

            //Normalize
            StemHistogram normalizedDistrib = new StemHistogram();
            for (String v : answers.keySet()) {
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
        private StemMap<StemMap<HashSet<MatchCase>>> keyToImages;
        private TableLoader tableLoader;
        private MatchCase matchCase;

        public MatchCase getMatchCase() {
            return matchCase;
        }


        private AbstractColumnMatcher colMatcherFrom;
        private AbstractColumnMatcher colMatcherTo;
        private boolean useNewlyDiscoveredKeys;
        private boolean closedWorld;
        public HashMap<MatchCase, Double> tableToPriorPartial = new HashMap<MatchCase, Double>();
        public HashMap<MatchCase, Double> tableToSchemaPriorPartial = new HashMap<MatchCase, Double>();
        public HashMap<MatchCase, HashSet<Pair<String, String>>> tableToAnswersPartial
                = new HashMap<MatchCase, HashSet<Pair<String, String>>>();
        public StemMap<StemMap<HashSet<MatchCase>>> result = new StemMap<StemMap<HashSet<MatchCase>>>();


        public TableChecker(StemMap<StemMap<HashSet<MatchCase>>> keyToImages,
                            TableLoader tableLoader, MatchCase matchCase,
                            AbstractColumnMatcher colMatcherFrom,
                            AbstractColumnMatcher colMatcherTo,
                            boolean useNewlyDiscoveredKeys, boolean closedWorld) {
            super();
            this.keyToImages = keyToImages;
            this.tableLoader = tableLoader;
            this.matchCase = matchCase;
            this.colMatcherFrom = colMatcherFrom;
            this.colMatcherTo = colMatcherTo;
            this.useNewlyDiscoveredKeys = useNewlyDiscoveredKeys;
            this.closedWorld = closedWorld;
        }


        @Override
        public void run() {
            try {
                String tableID = matchCase.tableID;
                int colF = matchCase.col1;
                int colT = matchCase.col2;

                Table webTable = tableLoader.loadTable(matchCase);

                Pair<List<Double>, List<Double>> checkResult = checkKnownTupleMapping(
                        webTable, colF, colT, knownExamples, closedWorld,
                        colMatcherFrom, colMatcherTo);


                List<Double> goods = checkResult.key;
                List<Double> bads = checkResult.value;


                if (assessTupleMapping(goods, bads, closedWorld)) {
                    //Transformation found

                    for (Tuple tup : webTable.getTuples()) {
                        String k = tup.getCell(colF).getValue();
                        String v = tup.getCell(colT).getValue(); //the transformed value

                        k = StemMap.getStem(k);
                        v = StemMap.getStem(v);

                        if (k != null) {
                            if (!keyToImages.containsKey(k) && !useNewlyDiscoveredKeys) {
                                continue;
                            }


                            if (!result.containsKey(k)) {
                                result.put(k, new StemMap<HashSet<MatchCase>>());
                            }
                            if (!result.get(k).containsKey(v)) {
                                result.get(k).put(v, new HashSet<MatchCase>());
                            }


                            if (!tableToAnswersPartial.containsKey(matchCase)) {
                                tableToAnswersPartial.put(matchCase, new HashSet<Pair<String, String>>());
                            }
                            tableToAnswersPartial.get(matchCase).add(new Pair<String, String>(k, v));

                            result.get(k).get(v).add(matchCase);
                        }
                    }
                    tableToPriorPartial.put(matchCase, (double) webTable.confidence);

                    double schemaMatchScore;
                    if (webTable.hasHeader()) {
                        String h1 = webTable.getColumnMapping().getColumnNames()[matchCase.col1];
                        String h2 = webTable.getColumnMapping().getColumnNames()[matchCase.col2];

                        schemaMatchScore = computeSchemaMatch(h1, h2);
                    } else {
                        schemaMatchScore = schemaMatchPrior;
                    }
                    tableToSchemaPriorPartial.put(matchCase, schemaMatchScore);
                } else {

                }

                //System.out.println("table " + tableID + " checked --- " + webTable.getNumRows() + " rows");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }


    }


    public void setGoodExample(String k, String v) {
        if (!knownExamples.containsKey(k)) {
            knownExamples.put(k, new StemHistogram());
        }
        knownExamples.get(k).setScoreOf(v, initialGoodExampleImportance);
        tableToAnswers.get(QUERY_TABLE_ID).add(new Pair<String, String>(k, v));


        if (!keyToImages.containsKey(k)) {
            keyToImages.put(k, new StemMap<HashSet<MatchCase>>());
        }
        if (!keyToImages.get(k).containsKey(v)) {
            keyToImages.get(k).put(v, new HashSet<MatchCase>());
        }
        keyToImages.get(k).get(v).add(QUERY_TABLE_ID);


    }


    public void setBadExample(String k, String v) {

        if (!knownExamples.containsKey(k)) {
            knownExamples.put(k, new StemHistogram());
        }
        knownExamples.get(k).setScoreOf(v, -initialGoodExampleImportance);
        tableToAnswers.get(QUERY_TABLE_ID).add(new Pair<String, String>(k, v));

        if (!keyToImages.containsKey(k)) {
            keyToImages.put(k, new StemMap<HashSet<MatchCase>>());
        }
        if (!keyToImages.get(k).containsKey(v)) {
            keyToImages.get(k).put(v, new HashSet<MatchCase>());
        }
        keyToImages.get(k).get(v).add(QUERY_TABLE_ID);

        throw new UnsupportedOperationException("STILL NEED TO TUNE PUNISHMENT");
    }


    public void boostTables(StemMap<StemHistogram> result) {
        Histogram<String> tableToCount = new Histogram<>();
        int count = 0;
        for (String k : result.keySet()) {
            for (String v : result.get(k).getCountsUnsorted().keySet()) {
                count++;
                for (MatchCase matchCase : keyToImages.get(k).get(v)) {
                    tableToCount.increment(matchCase.tableID);
                }
            }
        }

        for (String tableID : tableToCount.getCountsUnsorted().keySet()) {
            double prior = VerticaBatchTableLoader.getPrior(tableID);
            VerticaBatchTableLoader.setPrior(tableID, prior + (1 - prior) * tableToCount.getScoreOf(tableID) * SMOOTHING_FACTOR_ALPHA / count);
        }

    }


    public StemMap<StemMap<HashSet<MatchCase>>> getProvenance() {
        return keyToImages;
    }

    /**
     * Best table that is not the input
     *
     * @param k
     * @param v
     * @return
     */
    public MatchCase getBestTableForAnswer(String k, String v) {
        HashSet<MatchCase> tables = keyToImages.get(k).get(v);
        double bestRating = 0;
        MatchCase bestTable = null;
        for (MatchCase table : tables) {
            if (tableToRating.get(table) > bestRating) {
                bestTable = table;
                bestRating = tableToRating.get(table);
            }
        }

        return bestTable;
    }

}