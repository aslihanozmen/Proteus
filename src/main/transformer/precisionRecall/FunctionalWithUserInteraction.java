package main.transformer.precisionRecall;

import au.com.bytecode.opencsv.CSVReader;
import main.transformer.lookup.LookupTransformation;
import main.transformer.model.csv.CsvLookupSource;
import org.apache.commons.io.FileUtils;
import org.magicwerk.brownies.collections.GapList;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static main.dataXFormerDriver.examplePairs;
import static main.dataXFormerDriver.groundTruthTransformer;
import static main.transformer.precisionRecall.Functional.findCandidateKeysForATable;
import static main.transformer.precisionRecall.Indirect.findCandidateKeys;


public class FunctionalWithUserInteraction {

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
                   // csvLookupSource.setEditDistance(0.4);

                    LookupTransformation lookupTransformation = new LookupTransformation(csvLookupSource);
                    //Providing examples
                    transformationLookup(lookupTransformation, isMultiCol, index, tableFile, records);
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
                System.out.println("Providing Example... " + Arrays.toString(examplePairs.get(0)));
                try {
                    provideExample(lookupTransformation, Collections.singletonList(examplePairs.get(0)[0]), examplePairs.get(0)[1], exampleDir);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("It could not provide example!" + e.getMessage());
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

// FIXME Add parallelization
            //            AtomicInteger index = new AtomicInteger();
//            ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//            List<Callable<Object>> callable = records.stream()
//                    .map(record -> (Callable<Object>) () -> {
            // for (String[] record : records) {
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
//                if (filterUsedExamplePairs(examplePairs, input, expectedOutput) == null &&
//                        filterUsedExamplePairs(foundMatches, input, expectedOutput) == null) {
                System.out.println("Finding Match for " + inputs);
                try {
                    String output;
                    // TODO To experiment user-interaction
                    if (index == 0) {
                       lookupTransformation.findMultipleInputMatch(inputs, true);
                        output = lookupTransformation.findRealDataWithTransformation(inputs);
                    } else {
                        output = lookupTransformation.findRealDataWithTransformation(inputs);
                    }
                //    output =  lookupTransformation.findMultipleInputMatch(inputs, false);
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
                        // FIXME Related Macth
                        notFound++;
                        System.out.println("could not find a match for " + inputs + " expected " + expectedOutput);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("could not find a match for " + inputs + " expected " + expectedOutput);
                }
            }
            records.removeAll(foundValues);
            // }
            //      return null;
            //   }).collect(Collectors.toList());
//            executor.invokeAll(callable);
//            executor.shutdownNow();
//            callable.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static String[] listFilesUsingDirectoryStream(String dir) throws IOException {
        List<String> fileList = new LinkedList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir))) {
            for (Path path : stream) {
                fileList.add(path
                        .toString());
            }
        }
        return fileList.toArray(new String[fileList.size()]);
    }

    public static String[] listTablesToCheckCsvFiles(String dir) throws IOException {
        List<String> fileList = new LinkedList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir))) {
            for (Path path : stream) {
                if (!(path.toString().contains("knownExamples.csv"))
                        && !(path.toString().contains("missedValues.csv"))) {
                    fileList.add(path
                            .toString());
                }
            }
        }
        return fileList.toArray(new String[fileList.size()]);
    }

    public static String[] getKnownExampleAndResultFiles(final File folder) {
        List<String> filenames = new LinkedList<>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.getName().contains("knownExamples.csv")
                    || fileEntry.getName().contains("results.csv"))
                filenames.add(fileEntry.toString());
        }
        return filenames.toArray(new String[filenames.size()]);
    }

    public static String returnInputCSVFileName(final File folder, String CSVFileDir) {
        for (final File fileEntry : folder.listFiles()) {
            if (CSVFileDir.contains(fileEntry.getName()))
                return fileEntry.getName();
        }
        return null;
    }

    public static void createDirectory(String dirPath) throws IOException {
        Path path = Paths.get(dirPath);
        if (!Files.exists(Paths.get(dirPath))) {
            Files.createDirectory(path);
            System.out.println("Directory is created!");
        }
    }

}
