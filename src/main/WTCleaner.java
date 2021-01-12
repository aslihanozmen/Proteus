package main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.queryparser.classic.ParseException;

import enrichment.ValueAlternatives;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import model.BatchTableLoader;
import model.EnrichedSubTable;
import model.Table;
import model.Tuple;
import model.VerticaBatchTableLoader;
import model.multiColumn.MultiColTableLoader;
import query.multiColumn.MultiColTableQuerier;
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
 * @author John Morcos
 * Supports set semantics, where each value is also composite
 */
public class WTCleaner {
    public static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
    public static int COVERAGE_THRESHOLD = 10;
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
    public static final MultiColMatchCase QUERY_TABLE_ID = new MultiColMatchCase("main/java/query",
            new int[]{0}, new int[]{1});
    public static final int MAX_ITERS_FOR_EM = 100;

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
    private int inductiveIters;
    private boolean useNewlyDiscoveredKeys;
    private boolean augment;
    private ValueAlternatives va;

    /**
     * @param colsFrom
     * @param queryBuilder
     * @param tableLoader
     * @param useNewlyDiscoveredKeys
     * @return
     * @throws IOException
     * @throws ParseException
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws SQLException
     * @throws FileNotFoundException
     */
    public WTCleaner(Table inputTable, int[] colsFrom, MultiColTableQuerier queryBuilder,
                     final MultiColTableLoader tableLoader, String[] keywords,
                     final boolean useNewlyDiscoveredKeys) {
        this.inputTable = inputTable;
        this.colsFrom = colsFrom;
        this.queryBuilder = queryBuilder;
        this.tableLoader = tableLoader;
        this.useNewlyDiscoveredKeys = useNewlyDiscoveredKeys;

        for (Tuple t : inputTable.getTuples()) {
            List<String> k = t.getValuesOfCells(colsFrom);
            if (!knownExamples.containsKey(k)) {
                knownExamples.put(k, new StemMultiColHistogram());
            }

            keyToImages.put(k, new StemMultiColMap<HashSet<MultiColMatchCase>>());
        }

        tableToAnswers.put(QUERY_TABLE_ID, new HashSet<Pair<List<String>, List<String>>>());

        for (Tuple tuple : inputTable.getTuples()) {
            List<String> k = tuple.getValuesOfCells(colsFrom);

            HashSet<MultiColMatchCase> evidence = new HashSet<MultiColMatchCase>();
            evidence.add(QUERY_TABLE_ID);
            tableToAnswers.get(QUERY_TABLE_ID)
                    .add(new Pair<List<String>, List<String>>(k, new ArrayList<String>()));
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

    public void enrich() {
        int iter = 0;
        StemMultiColMap<StemMultiColHistogram> lastKnownExamples = new StemMultiColMap<StemMultiColHistogram>();

        done = false;
        message = "Initializing";
        answerToRating.putAll(knownExamples);
        // Clone knownExamples
        lastKnownExamples = cloneExamples(lastKnownExamples);
        // }
        StemMultiColMap<String> foundXs = new StemMultiColMap<String>();
        StemMultiColMap<String> newlyFoundXs = new StemMultiColMap<String>();

        // TODO collect table mapping results.
        message = "Iteration " + (iter + 1) + " - checking tables";
        ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREADS);

        threadPool.shutdown();
        awaitThreadPoolterminations(5, threadPool);

        message = "Iteration " + (iter + 1) + " - consolidating results";
        System.out.println(message);
        // Maximization: revise table ratings
        message = "Iteration " + (iter + 1) + " - adjusting answer scores";

        // Expectation: assess the scores and update examples
        try {
            // find all table concatenations here
            List<EnrichedSubTable> similarTables = findSimilarTables();
            printSimilarTables(similarTables);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void printSimilarTables(List<EnrichedSubTable> similarTables) {

    }

    private Collection<String>[] extractXs() {
        HashSet<String>[] xs = new HashSet[knownExamples.keySet().iterator().next().size()];
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
        HashSet<String>[] ys = new HashSet[knownExamples.values().iterator().next()
                .getCountsUnsorted().keySet().iterator().next().size()];

        for (int i = 0; i < ys.length; i++) {
            ys[i] = new HashSet<String>();
        }
        for (List<String> x : knownExamples.keySet())
            for (List<String> y : knownExamples.get(x).getCountsUnsorted().keySet()) {
                for (int i = 0; i < ys.length; i++) {
                    ys[i].add(y.get(i));
                }
            }
        return ys;
    }

    private void collectResultsFromTableChecker(ArrayList<TableChecker> tableCheckers,
                                                StemMultiColMap<String> foundXs, StemMultiColMap<String> newlyFoundXs) {
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

    private void loadTables(ArrayList<MultiColMatchCase> tablesToCheck) throws SQLException {
        if (tableLoader instanceof BatchTableLoader) {
            HashSet<String> tableIDs = new HashSet<String>();
            for (MultiColMatchCase MultiColMatchCase : tablesToCheck) {
                tableIDs.add(MultiColMatchCase.tableID);
            }
            ((BatchTableLoader) tableLoader).loadTables(tableIDs.toArray(new String[0]));
        }

    }

    private ArrayList<MultiColMatchCase> tableSetDifference(ArrayList<MultiColMatchCase> table1,
                                                            ArrayList<MultiColMatchCase> table2) {
        ArrayList<MultiColMatchCase> newXTables = new ArrayList<>();
        MultiColMatchCaseComparator comparator = new MultiColMatchCaseComparator();
        Collections.sort(table1, comparator);
        Collections.sort(table2, new MultiColMatchCaseComparator());
        for (int i = 0, j = 0; i < table1.size(); ) {
            if (j < table2.size()) {
                int comparison = comparator.compare(table1.get(i), table2.get(j));
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

    private List<EnrichedSubTable> findSimilarTables() throws Exception {
        List<EnrichedSubTable> enrichedSubTables = new ArrayList<>();
        Set<List<String>> values = new HashSet<>();

        ArrayList<MultiColMatchCase> xTables = queryBuilder.findTables(extractXs(),
                COVERAGE_THRESHOLD);
        loadTables(xTables);
        va = new ValueAlternatives(inputTable);
        int limit = 20;
        for (MultiColMatchCase tableTriple : xTables) {
            int[] xColumn = tableTriple.getCol1();
            if (limit < 1) {
                break;
            }
            limit--;
            Table table = tableLoader.loadTable(tableTriple);
            ArrayList<FunctionalDependency> fds = findFDsWithColAsLHS(table, xColumn[0]);
            for (FunctionalDependency fd : fds) {
                int[] ZIds = fd.getYIds().toArray();
                EnrichedSubTable enrichedSubTable = new EnrichedSubTable(tableTriple.tableID,
                        xColumn, ZIds, table);
                values.clear();
                // check if is FD:

                for (Tuple tup : table.getTuples()) {
                    List<String> x = tup.getValuesOfCells(xColumn);
                    if (knownExamples.containsKey(x)) {
                        List<String> z = tup.getValuesOfCells(ZIds); // the
                        if (z == null) {
                            continue;
                        }
                        enrichedSubTable.appendMapping(x, z);
                    }
                }
                enrichedSubTables.add(enrichedSubTable);
            }
            System.out.println("Intermediate table: " + tableTriple);
        }
        va.printAlternatives();
        return enrichedSubTables;

    }

    private void markMatches(TIntSet checkedIDs,
                             StemMultiColMap<StemMultiColHistogram> overlappingValues, Table inputTable, Table table,
                             int col, int foundCol, int[] xColumn) {
        HashMap<List<String>, StemMultiColHistogram> foundTableMap = createMapFromTable(table,
                new int[]{foundCol}, xColumn);
        int[] colIndices = {col};
        int[] foundColIndices = {foundCol};
        for (Tuple t : inputTable.getTuples()) {
            if (overlappingValues.containsKey(t.getValuesOfCells(colIndices))) {
                // check occurence in the new table
                for (Tuple t2 : table.getTuples()) {
                    if (checkedIDs.contains(t2.getTid())) {
                        // check each row in the found table only once
                        continue;
                    }
                    if (overlappingValues.containsKey(t2.getValuesOfCells(foundColIndices))) {
                        checkedIDs.add(t2.getTid());
                        // check values of the other columns
                        for (int i = 0; i < colsFrom.length; i++) {
                            if (colsFrom[i] != col) {
                                String newValue = t2.getCell(xColumn[i]).getValue();
                                va.addAlternativeCellValue(t.getTid(), colsFrom[i], newValue);
                            }

                        }
                    }

                }
            }
        }

    }

    private StemMultiColMap<StemMultiColHistogram> intersectColumns(
            HashMap<List<String>, StemMultiColHistogram> inputList,
            HashMap<List<String>, StemMultiColHistogram> foundList) {
        StemMultiColMap<StemMultiColHistogram> overlappingSet = new StemMultiColMap<>();
        for (List<String> k : inputList.keySet()) {
            if (foundList.containsKey(k)) {
                overlappingSet.put(k, new StemMultiColHistogram());
            }
        }
        return overlappingSet;
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
            for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                s.increment(v, knownExamples.get(k).getScoreOf(v));
            }
            lastKnownExamples.put(k, s);
        }
        return lastKnownExamples;
    }

    public void runEM() throws IOException, ParseException, InterruptedException,
            ExecutionException, SQLException {
        int oldInductiveIters = inductiveIters;
        boolean oldToConvergence = toConvergence;

        this.setInductiveIters(0); // No querying
        this.toConvergence = true;

        enrich();

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

        for (Pair<List<String>, List<String>> entry : tableToAnswers.get(MultiColMatchCase)) {
            List<String> k = entry.key;
            List<String> v = entry.value;

            coveredKeys.add(k);

            // for(String knownVal:
            // answerToRating.get(k).getCountsUnsorted().keySet())
            // {
            // if(colMatcherTo.match(v, knownVal))
            // {
            // goodSum += answerToRating.get(k).getScoreOf(knownVal);
            // }
            //
            // totalSum += answerToRating.get(k).getScoreOf(knownVal);
            // }

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
        double instanceBasedRating = (goodSum + prior * unseenWeightSum) * SMOOTHING_FACTOR_ALPHA
                / (totalSum + unseenWeightSum);

        // return instanceBasedRating;
        // double schemaScore = tableToSchemaScore.get(MultiColMatchCase);

        // double rating = (instanceBasedRating + schemaScore*schemaMatchWeight)
        // / (1 + schemaMatchWeight);
        double rating = instanceBasedRating;
        return rating;
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

    public void printResult(
            StemMultiColMap<StemMultiColMap<HashSet<MultiColMatchCase>>> keyToImages) {
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

    public HashMap<List<String>, Pair<List<Double>, List<Double>>> checkKnownTupleMapping(Table t,
                                                                                          int[] anchorCols, int[] depCols, StemMultiColMap<StemMultiColHistogram> knownExamples,
                                                                                          boolean closedWorld) {
        HashMap<List<String>, Pair<List<Double>, List<Double>>> keyToGoodAndBad = new StemMultiColMap<Pair<List<Double>, List<Double>>>();
        if (!fuzzyMatching) {
            StemMultiColMap<StemMultiColHistogram> tableAsTree = createMapFromTable(t, anchorCols,
                    depCols);

            for (List<String> k : tableAsTree.keySet()) {
                if (knownExamples.containsKey(k)) {
                    List<Double> good = new ArrayList<Double>();
                    List<Double> bad = new ArrayList<Double>();

                    for (List<String> v : tableAsTree.get(k).getCountsUnsorted().keySet()) {
                        if (knownExamples.get(k).containsKey(v)) {
                            good.add(knownExamples.get(k).getScoreOf(v));
                        } else {
                            // bad.add(knownExamples.get(k).getCountsSorted().get(0).value);
                            bad.add(1.0);
                        }
                    }

                    keyToGoodAndBad.put(k, new Pair<List<Double>, List<Double>>(good, bad));
                }

            }
        }

        return keyToGoodAndBad;

    }


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
            HashMap<List<String>, Pair<Double, Double>> localCompletenessCheck, boolean closedWorld,
            boolean weighted, boolean localCompleteness) {

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
    private static ArrayList<FunctionalDependency> findFDsWithColAsLHS(Table table,
                                                                       int anchorColIdx) throws Exception {
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
    private static ArrayList<FunctionalDependency> findFDsWithColInLHS(Table table,
                                                                       int anchorColIdx) throws Exception {
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
                boolean useNewlyDiscoveredKeys, boolean closedWorld, boolean useWeightedExamples,
                boolean localCompleteness) {
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
                            tableToAnswersPartial.get(multiColMatchCase)
                                    .add(new Pair<List<String>, List<String>>(k, v));

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
                    tableToSchemaPriorPartial.put(multiColMatchCase, schemaMatchScore);
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
            VerticaBatchTableLoader.setPrior(tableID, prior + (1 - prior)
                    * tableToCount.getScoreOf(tableID) * SMOOTHING_FACTOR_ALPHA / count);
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

}
