package main.transformer.precisionRecall;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BenchmarkCSV {

    public static void main(String[] arg) {

        try {
            String existingResultDir = "./benchmark/result";
            CSVPrinter resultCSVPrinter = new CSVPrinter(Files.newBufferedWriter(Paths.get("./benchmark/lookup_result_by_microsoft.csv")), CSVFormat.DEFAULT);
            resultCSVPrinter.printRecord("file_name", "not_found", "precision", "recall", "runtime/s");
            for (String resultFile : listCSVFilesForFolder(new File(existingResultDir))) {
                CSVParser csvParser = new CSVParser(Files.newBufferedReader(Paths.get(resultFile)), CSVFormat.DEFAULT);
                Map<String, String> recordsFromCsvFile = new HashMap<>();
                for (CSVRecord csvRecord : csvParser) {
                    recordsFromCsvFile.put(csvRecord.get(0), csvRecord.get(1));
                }
                long seconds = TimeUnit.MILLISECONDS.toSeconds(Long.parseLong(recordsFromCsvFile.get("runtime")));
                resultCSVPrinter.printRecord(parseDirNameForFile(resultFile), recordsFromCsvFile.get("not_found"),
                        recordsFromCsvFile.get("precision"), recordsFromCsvFile.get("recall"), Long.toString(seconds));
            }

            resultCSVPrinter.flush();
            resultCSVPrinter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static String[] listCSVFilesForFolder(final File folder) {
        List<String> filenames = new LinkedList<>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                for (final File resultFile : fileEntry.listFiles()) {
                    for(final File resultCsv : resultFile.listFiles()) {
                        if (resultCsv.getAbsolutePath().contains("results.csv")) {
                            filenames.add(resultCsv.getAbsolutePath());
                        }
                    }
                }
            }
        }
        return filenames.toArray(new String[filenames.size()]);
    }

    public static String parseDirNameForFile(String filePath) {
        Pattern p = Pattern.compile("[\\w-]+\\.");
        Matcher m = p.matcher(filePath);
        while(m.find()) {
            String fileName = m.group(0);
            System.out.println("Regex: " + fileName);
            return fileName;
        }
        return "";
    }
}