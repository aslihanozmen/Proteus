package main;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import ch.epfl.lara.synthesis.stringsolver.StringSolver;
import com.google.common.collect.Lists;
import main.transformer.precisionRecall.Functional;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.magicwerk.brownies.collections.GapList;
import test.GeneralTests;
import test.IndirectMatchingTest;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static main.transformer.precisionRecall.Functional.getRecordsOfCSV;

public class dataXFormerDriver {

    public static int numberOfExamplePairs = 0;
    public static boolean isUserInvolved = false;
    public static List<String[]> groundTruthTransformer = new GapList<>();
    public static List<String[]> examplePairs = new GapList<>();
    private static int indexForExamplePairs = 3;

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        if (args.length < 4) {
            System.out.println("params needed: filename inputcolumns outputcolumns mode: persons.txt 0,1 2 F");
            System.exit(123);
        }

        String file = args[0];

        String[] fromStrings = args[1].split(",");
        int[] colsFrom = new int[fromStrings.length];
        for (int i = 0; i < fromStrings.length; i++)
            colsFrom[i] = Integer.parseInt(fromStrings[i]);

        String[] toStrings = args[2].split(",");
        int[] colsTo = new int[toStrings.length];
        for (int i = 0; i < toStrings.length; i++)
            colsTo[i] = Integer.parseInt(toStrings[i]);

        String mode = args[3];
        int seed = 0;

        if (args.length == 5) {
            seed = Integer.parseInt(args[4]);
        } else {
            Random rnd = new Random();
            seed = rnd.nextInt();
        }

        WTTransformerMultiColSets.useOpenrank = true;
        GeneralTests tests = new GeneralTests();
        IndirectMatchingTest testsIndirect = new IndirectMatchingTest();
        tests.init();
        testsIndirect.init();

        if (mode.equals("NF")) {
            tests.testNonFunctionalMultiColSets(file, colsFrom, colsTo, seed);
        } else if (mode.equals("F")) {
            tests.testMultiColSets(file, colsFrom, colsTo, seed);
            long end = System.currentTimeMillis();
            System.out.println("Overall Runtime: " + (end - start));
        } else if (mode.equals("SF")) {
            groundTruthTransformer = getRecordsOfCSV(file);
            tests.testMultiColSetsSyntactical(prepareInputForDataXFormer(file), colsFrom, colsTo, seed, numberOfExamplePairs);
            long end = System.currentTimeMillis();
            System.out.println("Overall Runtime: " + (end - start));
            System.exit(0);
        } else if (mode.equals("SFU")) {
            indexForExamplePairs = 2;
            isUserInvolved = true;
            groundTruthTransformer = getRecordsOfCSV(file);
            tests.testMultiColSetsSyntactical(prepareInputForDataXFormer(file), colsFrom, colsTo, seed, numberOfExamplePairs);
            long end = System.currentTimeMillis();
            System.out.println("Overall Runtime: " + (end - start));
            System.exit(0);
        }  else if (mode.equals("NSF")) {
            prepareForNonSyntacticGraphGeneration(file);
            tests.testMultiColSetsSyntactical(file, colsFrom, colsTo, seed, numberOfExamplePairs);
            long end = System.currentTimeMillis();
            System.out.println("Overall Runtime: " + (end - start));
            System.exit(0);
        } else if (mode.equals("SI")) {
            groundTruthTransformer = getRecordsOfCSV(file);
            testsIndirect.testIndirectMatchingSetsSyntactic(prepareInputForDataXFormer(file), colsFrom, colsTo, numberOfExamplePairs);
            long end = System.currentTimeMillis();
            System.out.println("Overall Runtime: " + (end - start));
            System.exit(0);
        } else if (mode.equals("S")) {
            applyOnlySyntacticManipulations(file, colsFrom);
        } else {
            System.out.println("Mode: " + mode + "unknown! F NF UF are supported.");
        }
    }

    private static void prepareForNonSyntacticGraphGeneration(String file) throws IOException {
        numberOfExamplePairs = 3;
        groundTruthTransformer = getRecordsOfCSV(file);
        int index = 0;
        List<String[]> removable = new GapList<>();
        for(String[] pair : groundTruthTransformer) {
            if(index < numberOfExamplePairs) {
                if(pair[0].startsWith("Column")) {
                    removable.add(pair);
                } else {
                    examplePairs.add(pair);
                    index++;
                }
            } else {
                break;
            }
        }
        groundTruthTransformer.removeAll(examplePairs);
        groundTruthTransformer.removeAll(removable);
    }

    private static void applyOnlySyntacticManipulations(String file, int[] colsFrom) throws IOException {
        List<String[]> records = getRecordsOfCSV(file);
        List<String[]> examplePairs = new GapList<>();
        int index = 0;
        for (String[] pair : records) {
            if (index < 3) {
                examplePairs.add(pair);
                index++;
            } else {
                break;
            }
        }
        records.removeAll(examplePairs);

        long startTime = System.currentTimeMillis();
        StringSolver ss = new StringSolver();
        ss.setLoopLevel(0);

        int nInputs = colsFrom.length;
        for (String[] pair : examplePairs) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < pair.length; i++) {
                sb.append(pair[i]);
                if (i != nInputs) {
                    sb.append(" | ");
                }
            }
            ss.add(sb.toString(), nInputs);
        }

        int correctValsFound = 0;
        int wrongValsFound = 0;

        for (String[] record : records) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < nInputs; i++) {
                sb.append(record[i]);
                if (i != (nInputs - 1)) {
                    sb.append(" | ");
                }
            }
            String output = ss.solve(sb.toString(), false);

            int nColumns = record.length;

            if (record[nColumns - 1].equals(output)) {
                correctValsFound++;
            } else if (!output.isEmpty()) {
                wrongValsFound++;
            }
        }

        long endTime = System.currentTimeMillis();
        long runtime = endTime - startTime;

        precisionRecall(correctValsFound, records.size(), runtime, wrongValsFound);
    }

    public static void precisionRecall(int correctValsFound, int totalCorrectVals, long runtime, int wrongValsFound) {

        double precision = correctValsFound * 1.0 / (correctValsFound + wrongValsFound);
        if (correctValsFound + wrongValsFound == 0) {
            precision = 0;
        }
        System.out.println("correctValsFound " + correctValsFound);
        System.out.println("wrongValsFound " + wrongValsFound);
        System.out.println("totalCorrectVals " + totalCorrectVals);
        System.out.println("precision " + precision);
        System.out.println("recall " + correctValsFound * 1.0 / totalCorrectVals);
        System.out.println("runtime " + runtime);
    }

    public static List<String[]> getRecordsOfCSV(String s) throws IOException {
        CSVReader correctFoundValues = new CSVReader(new FileReader(s));
        List<String[]> allCorrectFoundElements = correctFoundValues.readAll();
        correctFoundValues.close();
        return allCorrectFoundElements;
    }

    private static File createDirectory() {
        File dir = new File("./experiment/input");
        if (!dir.exists()) {
            if (dir.mkdirs()) {
                System.out.println("Directory for experiment is created");
            }
        }
        return dir;
    }


    public static Map<String, String> getSyntacticalMapping(String file) throws IOException {
        List<String> wordsKey;
        List<String> wordsValue;
        List<String> allInputs = new GapList<>();
        List<String> allOutputs = new GapList<>();
        LinkedHashMap<String, String> examplePairOriginal = new LinkedHashMap<>();
        LinkedHashMap<String, String> examplePairOriginalSyntactical = new LinkedHashMap<>();
        CSVParser csvParser = new CSVParser(Files.newBufferedReader(Paths.get(file)), CSVFormat.DEFAULT);
        int index = 0;
        for (CSVRecord csvRecordExamplePair : csvParser) {
            if (index < indexForExamplePairs) {
                String input = csvRecordExamplePair.get(0);
                String output = csvRecordExamplePair.get(1);

                String[] examplePair = new String[2];
                examplePair[0] = input;
                examplePair[1] = output;

                for (String[] pair : groundTruthTransformer) {
                    if (pair[0].equalsIgnoreCase(input)) {
                        groundTruthTransformer.remove(pair);
                        break;
                    }
                }
                examplePairs.add(examplePair);

                examplePairOriginal.put(input, output);
                allInputs.addAll(countWords(input));
                allOutputs.addAll(countWords(output));
                index++;
            } else {
                break;
            }
        }

        for (Map.Entry<String, String> entry : examplePairOriginal.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            wordsKey = countWords(key);
            wordsValue = countWords(value);

            if (wordsValue.size() == 1) {
                for (String wordKey : wordsKey) {
                    if (wordKey.length() > 1) {
                        examplePairOriginalSyntactical.put(wordKey, wordsValue.get(0));
                    }
                }
            } else if (wordsValue.size() == wordsKey.size()) {
                Set<String> keyDuplicates = findDuplicates(allInputs);
                Set<String> valueDuplicates = findDuplicates(allOutputs);
                wordsKey.removeAll(keyDuplicates);
                wordsValue.removeAll(valueDuplicates);
                for (int i = 0; i < wordsValue.size(); i++) {
                    examplePairOriginalSyntactical.put(wordsKey.get(i), wordsValue.get(i));
                }
            }
            wordsKey.clear();
            wordsValue.clear();
        }

        return examplePairOriginalSyntactical;
    }

    public static String prepareInputForDataXFormer(String file) throws IOException {
        Map<String, String> dataXFormerExamplePairs = getSyntacticalMapping(file);
        File dir = createDirectory();

        File inputDataXformerFile = new File(dir.getAbsolutePath() + "/" + "inputDataXFormer.csv");
        if (inputDataXformerFile.createNewFile()) {
            System.out.println("Recreating the file");
        } else {
            PrintWriter writer = new PrintWriter(inputDataXformerFile);
            writer.print("");
            writer.close();
        }

        String inputDataXFormerPath = inputDataXformerFile.getAbsolutePath();
        CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(new FileOutputStream(inputDataXFormerPath),
                StandardCharsets.UTF_8), ',', '"');

        csvWriter.writeNext("C1", "C2");

        for (Map.Entry<String, String> entry : dataXFormerExamplePairs.entrySet()) {
            numberOfExamplePairs++;
            csvWriter.writeNext(entry.getKey(), entry.getValue());
        }

        for (String[] entry : groundTruthTransformer) {
            csvWriter.writeNext(entry[0], entry[1]);
        }

        csvWriter.flush();
        csvWriter.close();

        return inputDataXFormerPath;

    }

    private static List<String> countWords(String input) {
        if (input == null || input.isEmpty()) {
            return null;
        }
        return Lists.newArrayList(input.split("\\s+"));
    }

    private static Set<String> findDuplicates(List<String> listContainingDuplicates) {
        final Set<String> setToReturn = new HashSet<>();
        final Set<String> set1 = new HashSet<>();

        for (String s : listContainingDuplicates) {
            if (!set1.add(s)) {
                setToReturn.add(s);
            }
        }
        return setToReturn;
    }
}


