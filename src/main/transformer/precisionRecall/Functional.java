package main.transformer.precisionRecall;

import au.com.bytecode.opencsv.CSVReader;
import main.transformer.lookup.LookupTransformation;
import main.transformer.lookup.bean.CandidateKey;
import main.transformer.model.csv.CsvLookupSource;
import org.apache.commons.io.FileUtils;
import org.magicwerk.brownies.collections.GapList;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static main.dataXFormerDriver.*;


public class Functional {

    private static int notFound = 0;
    private static int correctValsFound = 0;
    private static int wrongValsFound = 0;
    private static List<ExamplePair> foundMatches = new LinkedList<>();
    private static List<ExamplePair> wrongFoundValuesMap = new GapList<>();

    public static void main(String[] args) {
        try {

            precisionRecall("./experiment/tablesToCheck",
                    false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void precisionRecall(String dirPathTablesToCheck, boolean isMultiCol) throws Exception {

        for (String dir : listFilesUsingDirectoryStream(dirPathTablesToCheck)) {

            for (String exampleDir : listFilesUsingDirectoryStream(dir)) {
                notFound = 0;
                correctValsFound = 0;
                wrongValsFound = 0;
                foundMatches.clear();
                wrongFoundValuesMap.clear();

                long startTime = System.currentTimeMillis();


                int index = 0;
                List<String[]> records = new GapList<>(groundTruthTransformer);
                for (String tableFile : listTablesToCheckCsvFiles(exampleDir)) {
                    CsvLookupSource csvLookupSource = new CsvLookupSource(tableFile);
                    //csvLookupSource.setEditDistance(0.1);
                    LookupTransformation lookupTransformation = new LookupTransformation(csvLookupSource);
                    //Providing examples
                    transformationLookup(lookupTransformation, isMultiCol, index, tableFile, records);

                    System.gc();
                }

                long endTime = System.currentTimeMillis();
                long runtime = endTime - startTime;


                precisionRecall(correctValsFound, (groundTruthTransformer.size()), runtime);
                System.gc();
            }
        }
    }

    public static List<String[]> getRecordsOfCSV(String s) throws IOException {
        CSVReader correctFoundValues = new CSVReader(new FileReader(s));
        List<String[]> allCorrectFoundElements = correctFoundValues.readAll();
        correctFoundValues.close();
        return allCorrectFoundElements;
    }


    public static void precisionRecall(int correctValsFound, int totalCorrectVals, long runtime) throws IOException {

        wrongValsFound = wrongFoundValuesMap.size();
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

        FileUtils.deleteDirectory(new File("./experiment"));
        System.exit(1);
    }

    private static void transformationLookup(LookupTransformation lookupTransformation, boolean isMultiCol, int index, String exampleDir, List<String[]> records) {
        try {
            for (String[] examplePair : examplePairs) {
                //Accessing values by column index
                System.out.println("Providing Example... " + Arrays.toString(examplePair));
                try {
                    provideExample(lookupTransformation, Collections.singletonList(examplePair[0]), examplePair[1], exampleDir);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("It could not provide example!" + e.getMessage());
                }
            }
            findMatch(lookupTransformation, isMultiCol, index, records);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void provideExample(LookupTransformation lookupTransformation, List<String> inputExample, String outputExample, String exampleDir) {
        try {
            lookupTransformation.provideMultipleInputsExampleWithCandidateKeys(inputExample, outputExample, findCandidateKeysForATable(exampleDir));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("It could not provide example!");
        }

    }

    private static void findMatch(LookupTransformation lookupTransformation, boolean isMultiCol, int index, List<String[]> records) {
        try {

            //Accessing values by column index
            List<String[]> foundValues = new ArrayList<>();
            for (String[] record : records) {
                List<String> inputs = new GapList<>();
                String expectedOutput;
                if (isMultiCol) {
                    inputs.add(record[0]);
                    inputs.add(record[1]);
                    expectedOutput = record[2];
                } else {
                    inputs.add(record[0]);
                    expectedOutput = record[1];
                }
                System.out.println("Finding Match for " + inputs);
                try {
                    String output;
                    output =  lookupTransformation.findMultipleInputMatch(inputs, false);
                    if (output != null && output.equalsIgnoreCase(expectedOutput)) {
                        correctValsFound += 1;
                        System.out.println("The found value:" + expectedOutput);
                        foundMatches.add(new ExamplePair(inputs, output));
                        index++;
                        foundValues.add(record);

                    } else if (output != null && !output.isEmpty()) {
                        System.out.println("Wrong Found Value for: " + inputs);
                        System.out.println("Expected Output: " + expectedOutput);
                        System.out.println("Found Output: " + output);
                        wrongValsFound += 1;
                        wrongFoundValuesMap.add(new ExamplePair(inputs, output));
                        foundValues.add(record);
                    } else {
                        notFound++;
                        System.out.println("could not find a match for " + inputs + " expected " + expectedOutput);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("could not find a match for " + inputs + " expected " + expectedOutput);
                }
            }
            records.removeAll(foundValues);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static Set<CandidateKey> findCandidateKeysForATable(String dir) {
        File file = new File(dir);
        String fileName = file.getName();
        Set<CandidateKey> candidateKeys = new HashSet<>();
        Pattern p = Pattern.compile("(?<=\\[)([^]]+)(?=])");
        Matcher m = p.matcher(fileName);
        while(m.find()) {
            CandidateKey ck = new CandidateKey(fileName, fileName + "-" + m.group(1));
            candidateKeys.add(ck);
        }
        return candidateKeys;
    }


    private static String[] listFilesUsingDirectoryStream(String dir) throws IOException {
        return getFileLists(dir);
    }

    private static String[] getFileLists(String dir) throws IOException {
        List<String> fileList = new LinkedList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir))) {
            for (Path path : stream) {
                fileList.add(path
                        .toString());
            }
        }
        return fileList.toArray(new String[fileList.size()]);
    }

    private static String[] listTablesToCheckCsvFiles(String dir) throws IOException {
        return getFileLists(dir);
    }

}
