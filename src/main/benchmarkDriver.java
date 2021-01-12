package main;

import test.GeneralTests;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class benchmarkDriver {

    public static void main(String args[]) throws Exception {
        File folder = new File("/Users/aslihanozmen/Documents/DataTransformationDiscoveryThesis-master/benchmark/csvSyntactic/benchmark-stackoverflow");
        List<String> filenames = listFilesForFolder(folder);

        File tableToCheckFolder = new File("/Users/aslihanozmen/Documents/DataTransformationDiscoveryThesis-master/benchmark/syntacticBenchmarkResult/benchmark-stackoverflow");
        List<String> tableToCheckFileNames = listFilesUsingDirectoryStream(tableToCheckFolder);

        for(String inputFile : filenames) {
            boolean alreadyExist = false;
            for(String fileName : tableToCheckFileNames) {
                if(inputFile.contains(fileName)) {
                    alreadyExist = true;
                    break;
                }
            }
            /*if( (!inputFile.contains("test.csv"))  &&  (!inputFile.contains("Album2GenreQ.csv")) && (!inputFile.contains("prettycleaned_currencies.csv"))
                    && (!inputFile.contains("prettycleaned_iban.csv")) && (!inputFile.contains("prettycleaned_laureates"))) {*/

                if(!alreadyExist) {

                    int[] colsFrom = new int[]{0};
                    int[] colsTo = new int[]{1};
                    int seed = 0;
                    Random rnd = new Random();
                    seed = rnd.nextInt();
                    WTTransformerMultiColSets.useOpenrank = true;
                    GeneralTests tests = new GeneralTests();
                    tests.init();
                    tests.testMultiColSets(inputFile, colsFrom, colsTo, seed);
                }

           /* } */
        }


    }

    private static List<String> listFilesForFolder(final File folder) {
        List<String> filenames = new LinkedList<String>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                if(fileEntry.getName().contains(".csv"))
                    filenames.add(fileEntry.toString());
            }
        }

        return filenames;
    }

    private static List<String> listFilesUsingDirectoryStream(File folder) throws IOException {
        List<String> filenames = new LinkedList<String>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                filenames.add(fileEntry.getName());
            }
        }

        return filenames;
    }
}
