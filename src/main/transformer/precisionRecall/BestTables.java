package main.transformer.precisionRecall;

import au.com.bytecode.opencsv.CSVReader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BestTables {

    public static void main(String[] args) throws IOException {
        System.out.println("Best tables with least column amount: " + listFilesUsingDirectoryStream("/Users/aslihanozmen/online/LookupSyntactic/benchmark/tableToCheckBackup/CountryToCountryCode.csv/3"));
    }

    private static  Set<String> listFilesUsingDirectoryStream(String dir) throws IOException {
        Map<String, Integer> bestTables = new HashMap<>();
        Map<String, Integer> bestTablesWithRowCount = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir))) {
            for (Path path : stream) {
                FileInputStream fis = new FileInputStream(path.toString());
                InputStreamReader isr = new InputStreamReader(fis,
                        StandardCharsets.UTF_8);
                CSVReader csvReader = new CSVReader(isr);
                String[] header = csvReader.readNext(); // assuming first read
                if (header != null) {                     // and there is a (header) line
                    int columnCount = header.length;       // get the column count
                    bestTables.put(path.getFileName().toString(),columnCount);
                    bestTablesWithRowCount.put(path.getFileName().toString(),csvReader.readAll().size());
                }
            }

            System.out.println("All the tables: " + bestTables);

            Map.Entry<String, Integer> minEntry = null;
            for (Map.Entry<String, Integer> entry : bestTables.entrySet()) {
                if (minEntry == null || entry.getValue() < minEntry.getValue()) {
                    minEntry = entry;
                }
            }
            assert minEntry != null;
            int minCols = minEntry.getValue();
            System.out.println("Min Cols Number: " + minCols);
            Set<String> tables =  getKeys(bestTables, minCols);
            for(Map.Entry<String, Integer> candidate :  bestTablesWithRowCount.entrySet()) {
                for(String table : tables) {
                    if(table.equals(candidate.getKey())) {
                        System.out.println("Candidate: "  + candidate);
                    }
                }
            }
            return tables;
        }
    }


    private static <K, V> Set<K> getKeys(Map<K, V> map, V value) {
        Set<K> keys = new HashSet<>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (entry.getValue().equals(value)) {
                keys.add(entry.getKey());
            }
        }
        return keys;
    }
}
