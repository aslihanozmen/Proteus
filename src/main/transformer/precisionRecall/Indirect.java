package main.transformer.precisionRecall;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import main.transformer.lookup.LookupTransformation;
import main.transformer.lookup.bean.CandidateKey;
import main.transformer.model.csv.CsvLookupSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.magicwerk.brownies.collections.GapList;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Indirect {

    private static int notFound = 0;
    private static int correctValsFound = 0;
    private static int wrongValsFound = 0;
    private static List<ExamplePair> foundMatches = new LinkedList<>();
    private static List<ExamplePair> wrongFoundValuesMap = new GapList<>();

    public static void main(String[] args) {
        try {

            precisionRecall("./benchmarkShortTime/tablesToCheck",
                    "./benchmarkShortTime/functional",
                    "./benchmarkShortTime/result",
                    false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void precisionRecall(String dirPathTablesToCheck, String dirPathCSVInputFile, String dirResult, boolean isMultiCol) throws Exception {

        String[] resultDirs = listFilesUsingDirectoryStream(dirResult);
        for (String dir : listFilesUsingDirectoryStream(dirPathTablesToCheck)) {
            boolean isProcessed = false;
            if (resultDirs.length != 0) {
                for (String element : resultDirs) {
                    if (element.contains(dir.substring(dir.lastIndexOf('/') + 1))) {
                        isProcessed = true;
                        break;
                    }
                }
            }
            if (!isProcessed) {

                String inputCSVFileName = returnInputCSVFileName(new File(dirPathCSVInputFile), dir);

                System.out.println("File: " + inputCSVFileName);
                String inputCSVFilePath = dirPathCSVInputFile + '/' + inputCSVFileName;
                String resultCSVFilePath = dirResult + '/' + inputCSVFileName;

                Files.createDirectories(Paths.get(resultCSVFilePath));

                //int numberOfRecordsInInputCSVFile = getRecordsOfCSV("/Users/aslihanozmen/online/LookupSyntactic/benchmark/functional/prettycleaned_airport_codes_internalGT2.csv").size();

                List<String[]> recordsOfNotFoundVals = Collections.synchronizedList(new ArrayList<>(getRecordsOfCSV(inputCSVFilePath)));
               // recordsOfNotFoundVals = getRecordsOfCSV(inputCSVFilePath);
                //int numberOfRecordsInFirstNotFoundFromDataXFormer = recordsOfNotFoundVals.size();

                for (String exampleDir : listFilesUsingDirectoryStream(dir)) {
                    //for (String relatedTable : listTablesToCheckCsvFiles(exampleDir)) {
                    notFound = 0;
                    correctValsFound = 0;
                    // FIXME get wrongVals from DataXFormer
                    wrongValsFound = 0;
                    foundMatches.clear();
                    wrongFoundValuesMap.clear();

                    // int totalCorrectVals = numberOfRecordsInInputCSVFile - 3;

                    int exampleCount = Integer.parseInt(exampleDir.substring(exampleDir.length() - 1));

                    Files.createDirectories(Paths.get(resultCSVFilePath + "/" + exampleCount));

                    CSVPrinter resultCSVPrinter = new CSVPrinter(Files.newBufferedWriter(Paths.get(resultCSVFilePath + "/" + exampleCount + "/results.csv")), CSVFormat.DEFAULT);

                    File csvDir = new File(exampleDir);

                    String[] tablesToCheckInCsv = getKnownExampleAndResultFiles(csvDir);
                    long startTime = System.currentTimeMillis();


//                    List<String[]> groundValues = getRecordsOfCSV(" ");
//                    List<ExamplePair> allFoundCorrectValues = new GapList<>();

                    int index = 0;

                    LookupTransformation lookupTransformation = new LookupTransformation(new CsvLookupSource(listTablesToCheckCsvFiles(exampleDir)));
                    //Providing examples
                    transformationLookup(inputCSVFilePath, tablesToCheckInCsv, lookupTransformation, recordsOfNotFoundVals, isMultiCol, index, exampleDir);

                    System.gc();

                    long runtime = System.currentTimeMillis() - startTime;

                    precisionRecall(correctValsFound, 7, resultCSVPrinter, runtime);
                    resultCSVPrinter.flush();
                    resultCSVPrinter.close();
                    System.gc();
                    System.exit(0);
                }
            }
        }
    }

    public static List<String[]> getRecordsOfCSV(String s) throws IOException {
        CSVReader correctFoundValues = new CSVReader(new FileReader(s));
        List<String[]> allCorrectFoundElements = correctFoundValues.readAll();
        correctFoundValues.close();
        return allCorrectFoundElements;
    }


    public static void precisionRecall(int correctValsFound, int totalCorrectVals, CSVPrinter resultCSVPrinter, long runtime) {

        wrongValsFound = wrongFoundValuesMap.size();
        double precision = correctValsFound * 1.0 / (correctValsFound + wrongValsFound);
        if (correctValsFound + wrongValsFound == 0) {
            precision = 0;
        }

        try {
            resultCSVPrinter.printRecord("correctValsFound", correctValsFound);
            resultCSVPrinter.printRecord("wrongValsFound", wrongValsFound);
            resultCSVPrinter.printRecord("totalCorrectVals", totalCorrectVals);
           // resultCSVPrinter.printRecord("not_found", notFound);
            resultCSVPrinter.printRecord("precision", precision);
            resultCSVPrinter.printRecord("recall", correctValsFound * 1.0 / totalCorrectVals);
            resultCSVPrinter.printRecord("runtime", runtime);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private static void transformationLookup(String inputCSVFile, String[] knownExampleWithResult, LookupTransformation lookupTransformation,
                                             List<String[]> records, boolean isMultiCol, int index, String exampleDir) {
        List<ExamplePair> examplePairs = new GapList<>();
        for (String exampleResult : knownExampleWithResult) {
            try {
                if (exampleResult.contains("knownExamples.csv")) {
                    CSVParser csvParser = new CSVParser(Files.newBufferedReader(Paths.get(exampleResult)), CSVFormat.DEFAULT);
                    for (CSVRecord csvRecordExamplePair : csvParser) {
                        List<String> inputs = new GapList<>();
                        if (isMultiCol) {
                            inputs.add(csvRecordExamplePair.get(0));
                            inputs.add(csvRecordExamplePair.get(1));
                            examplePairs.add(new ExamplePair(inputs, csvRecordExamplePair.get(2)));
                        } else {
                            inputs.add(csvRecordExamplePair.get(0));
                            examplePairs.add(new ExamplePair(inputs, csvRecordExamplePair.get(1)));
                        }
                    }
                    for (ExamplePair examplePair : examplePairs) {
                        //Accessing values by column index
                        System.out.println("Providing Example... " + examplePair);
                        try {
                            provideExample(lookupTransformation, examplePair.getInputs(), examplePair.getOutput(), exampleDir);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("It could not provide example!" + e.getMessage());
                        }
                    }
                    csvParser.close();
                    findMatch(inputCSVFile, lookupTransformation, isMultiCol, records, index);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void provideExample(LookupTransformation lookupTransformation, List<String> inputExample, String outputExample, String exampleDir) {
        try {
            //lookupTransformation.provideMultipleInputsExampleWithCandidateKeys(inputExample, outputExample, findCandidateKeys(exampleDir));
            lookupTransformation.provideMultipleInputsExample(inputExample, outputExample);

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("It could not provide example!");
        }

    }

    private static void findMatch(String inputCSVFile, LookupTransformation lookupTransformation, boolean isMultiCol, List<String[]> records, int index) {
        try {
            String dirPath = "./benchmarkShortTime/correctFoundValues";
            File file = new File(dirPath);

            if (!file.exists()) {
                createDirectory(dirPath);
            }
            File inputFile = new File(inputCSVFile);
            File correctFoundValueFile = new File(file.getAbsolutePath() + "/" + inputFile.getName());
            if (correctFoundValueFile.createNewFile()) {
                System.out.println("Recreating the file");
            } else {
                PrintWriter writer = new PrintWriter(correctFoundValueFile);
                writer.print("");
                writer.close();
            }

            CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(new FileOutputStream(correctFoundValueFile.getAbsoluteFile()),
                    StandardCharsets.UTF_8), ',', '"');

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
                    if (index == 0) {
                       lookupTransformation.findMultipleInputMatch(inputs, true);
                        output = lookupTransformation.findRealDataWithTransformation(inputs);
                    } else {
                        output = lookupTransformation.findRealDataWithTransformation(inputs);
                    }
                  //output = lookupTransformation.findMultipleInputMatch(inputs, false);
                    if (output != null && output.equalsIgnoreCase(expectedOutput)) {
                        correctValsFound += 1;
                        System.out.println("The found value:" + expectedOutput);
                        foundMatches.add(new ExamplePair(inputs, output));
                        csvWriter.writeNext(inputs.get(0), output);
                        index ++;
                        foundValues.add(record);

                    } else if (output != null && !output.isEmpty()) {
                        System.out.println("Wrong Found Value for: " + inputs);
                        System.out.println("Expected Output: " + expectedOutput);
                        System.out.println("Found Output: " + output);
                        wrongValsFound += 1;
                        foundValues.add(record);
                        wrongFoundValuesMap.add(new ExamplePair(inputs, output));
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
            csvWriter.flush();
            csvWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    private static ExamplePair filterUsedExamplePairs(List<ExamplePair> examplePairs, String input, String expectedOutput) {
//        return examplePairs.stream()
//                .filter(pair -> input.equalsIgnoreCase(pair.getInputs()))
//                .filter(pair -> expectedOutput.equalsIgnoreCase(pair.getOutput()))
//                .findAny()
//                .orElse(null);
//    }

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

    public static  Set<CandidateKey> findCandidateKeys(String dir) {
        List<String> filenames = new LinkedList<>();
        File file = new File(dir);
        for (final File fileEntry : Objects.requireNonNull(file.listFiles())) {
            if (!fileEntry.getName().contains("knownExamples.csv")){
                filenames.add(fileEntry.getName());
            }
        }

        Set<CandidateKey> candidateKeys = new HashSet<>();
        for(String fileName : filenames) {
            Pattern p = Pattern.compile("(?<=\\[)([^]]+)(?=])");
            Matcher m = p.matcher(fileName);
            while(m.find()) {
                CandidateKey ck = new CandidateKey(fileName, fileName + "-" + m.group(1));
                candidateKeys.add(ck);
            }

        }
        return candidateKeys;
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
