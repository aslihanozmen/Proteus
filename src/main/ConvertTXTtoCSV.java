package main;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConvertTXTtoCSV {

    public static void main(String args[]) throws IOException {

        String txtPath = "/Users/aslihanozmen/Documents/DataTransformationDiscoveryThesis-master/benchmark/microsoft/benchmark-bing-query-logs";
        String csvPath = "/Users/aslihanozmen/Documents/DataTransformationDiscoveryThesis-master/benchmark/csvSyntactic/benchmark-bing-query-logs";

        for (File file : new File(txtPath).listFiles()) {
            if (file.isFile()) {
                String filename = file.getName();     // full file name
                System.out.println(filename);
                int iend = filename.indexOf('.'); //this finds the first occurrence of "." in string thus giving you the index of where it is in the string

                if (iend != -1) {
                    String subString = filename.substring(0, iend);
                    final Path txt = Paths.get(txtPath).resolve(subString + ".txt");
                    final Path csv = Paths.get(csvPath).resolve(subString + ".csv");
                    if(!checkFileExistInDir(new File(csvPath).listFiles(), subString)) {

                        try (
                                final Stream<String> lines = Files.lines(txt);
                                final PrintWriter pw = new PrintWriter(Files.newBufferedWriter(csv, StandardOpenOption.CREATE_NEW))) {
                            pw.println("COLUMN1, COLUMN2");
                            lines.map((line) -> line.split("[ \\t]{2,}")).
                                    map((line) -> Stream.of(line).collect(Collectors.joining(","))).
                                    forEach(pw::println);
                        }
                    }
                }
            }
        }

    }

    public static boolean checkFileExistInDir(File[] listOfCsvFiles, String subString ) {
        for(File csvFile : listOfCsvFiles) {
            if(csvFile.getName().contains(subString)) {
                return true;
            }
        }
        return false;
    }
}
