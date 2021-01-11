package test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import main.WTEnricher;
import main.WTTransformerIndirect;
import org.junit.Before;
import org.junit.Test;

import com.vertica.jdbc.VerticaConnection;
import au.com.bytecode.opencsv.CSVWriter;
import model.EnrichedSubTable;
import model.Table;
import model.Tuple;
import model.columnMapping.AbstractColumnMatcher;
import model.multiColumn.VerticaMultiColBatchTableLoader;
import query.multiColumn.MultiColTableQuerier;
import query.multiColumn.MultiColVerticaQuerier;
import util.MultiColMatchCase;
import util.Pair;
import util.StemHistogram;
import util.StemMap;
import util.StemMultiColHistogram;
import util.StemMultiColMap;

public class EnrichingTest {

    public static StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth = null;
    private Properties verticaProperties = null;


    @Before
    public void init() {
        try {
            verticaProperties.load(new FileInputStream("/vertica.properties"));
        } catch (IOException e) {
//            e.printStackTrace();
            System.err.println("Properties file not found");
        }
        if (verticaProperties == null) {
            verticaProperties = new Properties();
            verticaProperties.setProperty("user", "user");
            verticaProperties.setProperty("password", "password");
            verticaProperties.setProperty("database", "database");
            verticaProperties.setProperty("database_host", "database_host");
        }

    }

    @Test
    public void testEnrichment(String file, int[] colsFrom) throws Exception {


        StemMultiColMap<StemMultiColMap<HashSet<String>>> groundTruth = new StemMultiColMap<>();
        HashMap<List<String>, List<Tuple>> keysToTuples = new HashMap<>();

        Table fullTable = new Table(file);

        for (Tuple tuple : fullTable.getTuples()) {
            List<String> k = tuple.getValuesOfCells(colsFrom);

            if (!keysToTuples.containsKey(k)) {
                keysToTuples.put(k, new ArrayList<>());
            }
            keysToTuples.get(k).add(tuple);
        }

        EnrichingTest.groundTruth = groundTruth;//TODO remove

        WTTransformerIndirect.COVERAGE_THRESHOLD = 2;
        WTTransformerIndirect.verbose = false;


        // For openWorld
        List<Tuple> tuples = new ArrayList<>();
        for (List<Tuple> tupleList : keysToTuples.values()) {
            tuples.addAll(tupleList);
        }

        List<List<String>> keys = new ArrayList<List<String>>(groundTruth
                .keySet().size());
        keys.addAll(keysToTuples.keySet());

        String[] h1 = fullTable.getColumnMapping().getColumnNames(colsFrom);

        for (int j = 0; j < h1.length; j++) {
            if (h1[j] == null || h1[j].equalsIgnoreCase("COLUMN" + colsFrom[j])) {
                h1[j] = null;
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

        WTEnricher enricher = new WTEnricher(
                fullTable, colsFrom,
                queryBuilder, tableLoader, null);
        // TODO edit functional test.
        printSimilarTables(enricher.enrich(), file, fullTable, colsFrom);
        con.close();

    }

    private void printSimilarTables(List<EnrichedSubTable> similarTables, String file, Table fullTable, int[] colsFrom) throws IOException {

        CSVWriter writer = new CSVWriter(new FileWriter(new File(file.replace(".csv", "") + "_enriched.csv")));
        String[] enrichedHeader = new String[similarTables.size() + 1];
        enrichedHeader[0] = "pivotColumn";
        for (int i = 0; i < similarTables.size(); i++) {
            enrichedHeader[i + 1] = similarTables.get(i).getColumnHeader();
        }
        String[] enrichedREcord = new String[similarTables.size() + 1];
        writer.writeNext(enrichedHeader);
        for (Tuple tuple : fullTable.getTuples()) {
            ArrayList<String> pivotValue = tuple.getValuesOfCells(colsFrom);
            for (int i = 0; i < similarTables.size(); i++) {
                if (similarTables.get(i).getxToZMapping().containsKey(pivotValue)) {
                    enrichedREcord[i + 1] = similarTables.get(i).getxToZMapping().get(pivotValue).getCountsUnsorted().keySet().toArray(new String[0])[0];
                } else {
                    enrichedREcord[i + 1] = "";
                }
            }
            writer.writeNext(enrichedREcord);
        }
        writer.flush();
        writer.close();
    }



    public void testInputEffectEnrichment(int exampleCounts,
                                          HashMap<List<String>, List<Tuple>> keysToTuples, int[] colsFrom,
                                          Table fullTable,
                                          AbstractColumnMatcher[] matchersFrom) {
        throw new UnsupportedOperationException();
    }


    private Table createInputTable(Table fullTable, List<List<String>> keys,
                                   HashMap<List<String>, List<Tuple>> keysToTuples, int exampleCount,
                                   List<Tuple> tuples) {
        Table inputTable = new Table();
        inputTable.setSchema(fullTable.getColumnMapping());

        for (int j = 0; j < exampleCount; j++) {
            List<String> key = keys.get(j);

            // add all its tuples
            for (Tuple tuple : keysToTuples.get(key)) {
                inputTable.addTuple(new Tuple(tuple));
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
        // for(String k : knownExamples.keySet())
        // {
        // for(String v : knownExamples.get(k).getCountsUnsorted().keySet())
        // {
        // System.out.println(k + " -> " + v);
        // }
        // }

        int correctValsFound = 0;
        int wrongValsFound = 0;
        int totalCorrectVals = 0;
        for (List<String> k : result.keySet()) {
            if (groundTruth.containsKey(k) && knownExamples.containsKey(k)) {
                for (List<String> val : result.get(k).keySet()) {
                    // if(result.get(k).get(val).size() == 1
                    // &&
                    // result.get(k).get(val).iterator().next().equals("query"))
                    // {
                    // continue;
                    // }
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

        // System.out.println(correctValsFound + "\t" + wrongValsFound + "\t" +
        // totalCorrectVals);
        return new Pair<Double, Double>(precision, recall);
    }

}