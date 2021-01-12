package main.transformer.model.csv;

import main.transformer.candidatekey.KeyFinder;
import main.transformer.lookup.bean.CandidateKey;
import main.transformer.model.LookupSource;
import main.transformer.util.LevenshteinDistanceStrategy;
import model.Table;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.magicwerk.brownies.collections.GapList;
import util.fd.tane.FunctionalDependency;
import util.fd.tane.TANEjava;

import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvLookupSource implements LookupSource {

    private List<Table> tableList = new GapList<>();
    private static final double FD_ERROR = 0.10;
    private Map<String, List<Map<String, String>>> allFiles = new HashMap<>();
    private ConcurrentHashMap<String, Set<String>> columnsPerValue = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Set<String>> columnsPerFile = new ConcurrentHashMap<>();
    private boolean isMultiCol;
    private double editDistance = 0.77;

    public List<Table> getTableList() {
        return tableList;
    }

    public CsvLookupSource(String... files) throws Exception {
        int numberOfFiles = files.length;
        if (numberOfFiles < 2) {
            numberOfFiles = 2;
        }
        ExecutorService pool = Executors.newFixedThreadPool(numberOfFiles / 2);
        List<Callable<Object>> callable = Stream.of(files)
                .map(file -> (Callable<Object>) () -> {
                    File f = new File(file);
                    LinkedList<Map<String, String>> csvData = new LinkedList<>();
                    for (CSVRecord record : CSVFormat.DEFAULT.parse(new FileReader(f))) {
                        int index = 0;
                        Map<String, String> line = new HashMap<>();
                        for (String value : record) {
                            String column = f.getName() + "-" + index;
                            line.put(column, value);
                            columnsPerValue.computeIfAbsent(value, val -> new HashSet<>()).add(column);
                            columnsPerFile.computeIfAbsent(f.getName(), ff -> new HashSet<>()).add(column);
                            index++;
                        }
                        csvData.add(line);
                    }
                    Table table = new Table(file, csvData.size(), false, f.getName());
                    tableList.add(table);
                    allFiles.put(f.getName(), csvData);
                    return null;
                }).collect(Collectors.toList());
        pool.invokeAll(callable);
        pool.shutdownNow();
        callable.clear();
    }

    public double getEditDistance() {
        return editDistance;
    }

    public void setEditDistance(double editDistance) {
        this.editDistance = editDistance;
    }

    public boolean isMultiCol() {
        return isMultiCol;
    }

    public void setMultiCol(boolean multiCol) {
        isMultiCol = multiCol;
    }


    @Override
    public Collection<String> getColumnNames(String tableName) {
        return columnsPerFile.get(tableName);
    }

    @Override
    public Collection<String> getTables() {
        return allFiles.keySet();
    }

    //TODO like levenstein and substring
    @Override
    public Set<CandidateKey> findDataKeys(Set<CandidateKey> keys, List<String> input) {
        HashSet<CandidateKey> result = new HashSet<>();
        List<Map<String, String>> table;
        for (CandidateKey key : keys) {
            table = allFiles.get(key.getTable());
            if (table!= null) {
                table.stream()
                        .filter(line -> input.contains(line.get(key.getColumn())))
                        .findAny()
                        .ifPresent(line -> result.add(key));
            }
        }
        return result;
    }

    @Override
    public String query(Set<String> srcKey, Map<String, List<String>> param, CandidateKey destKey) {
        for (String src : srcKey) {
            if (param.get(src) == null) {
                return null;
            }
        }
        for (Map<String, String> line : allFiles.get(destKey.getTable())) {
            boolean matches = true;
            for (String src : srcKey) {
                if (line.values().stream().noneMatch(param.get(src)::contains)) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return line.get(destKey.getColumn());
            }
        }
        param.clear();
        srcKey.clear();
        return null;
    }

    @Override
    public List<String> queryAllSolution(Set<String> srcKey, Map<String, List<String>> param, CandidateKey destKey) {
        for (String src : srcKey) {
            if (param.get(src) == null) {
                return Collections.emptyList();
            }
        }
        List<String> result = new GapList<>();
        // TODO - try to create subtask and make it parallel
        for (Map<String, String> line : allFiles.get(destKey.getTable())) {
            boolean matches = true;
            for (String src : srcKey) {
                if (line.values().stream().noneMatch(param.get(src)::contains)) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                result.add(line.get(destKey.getColumn()));
            }
        }
        if (!result.isEmpty())
            return result;
        else {
            return Collections.singletonList(null);
        }
    }

    @Override
    public Map<String, String> findAll(String input, String table) {

        List<Map<String, String>> tableData = allFiles.get(table);
        for (Map<String, String> line : tableData) {
            if (line.containsValue(input)) {
                Map<String, String> result = new HashMap<>();
                line.forEach(result::put);
                return result;
            }
        }
        return Collections.emptyMap();
    }

    // Ranking : prefer lookup expressions that match longer strings in table entries for indexing
    // than the ones that match shorter strings
    @Override
    public Map<String, CandidateKey> allDataKeys(Set<CandidateKey> keys, List<String> inputs) {
        Map<String, CandidateKey> result = new HashMap<>();
        Map<Map<String,CandidateKey>, Double> scoredValues = new HashMap<>();
        LevenshteinDistanceStrategy strategy = new LevenshteinDistanceStrategy();
        int length = 0;
        for (CandidateKey key : keys) {
            length = getLength(inputs, result, length, key, strategy, scoredValues);
        }
        //keys.clear();
        if(result.isEmpty() && !scoredValues.isEmpty()) {
            Map<String, CandidateKey> bestScoredValue = Collections.max(scoredValues.entrySet(), Comparator.comparingDouble(Map.Entry::getValue)).getKey();
            result.put(bestScoredValue.keySet().iterator().next(), bestScoredValue.values().iterator().next());
        }
        return result;
    }

    public Map<String, CandidateKey> allDataKeysWithProvidedCandidateKeys(Set<CandidateKey> keys, List<String> inputs) {
        Map<String, CandidateKey> result = new HashMap<>();
        Map<Map<String,CandidateKey>, Double> scoredValues = new HashMap<>();
        LevenshteinDistanceStrategy strategy = new LevenshteinDistanceStrategy();
        int length = 0;
        for (CandidateKey key : keys) {
            length = getLength(inputs, result, length, key, strategy, scoredValues);
        }
        //keys.clear();
        if(!scoredValues.isEmpty()) {
            double max  = Collections.max(scoredValues.values());
            for(Map.Entry<Map<String, CandidateKey>, Double> value : scoredValues.entrySet()) {
                if(value.getValue() == max) {
                    result.put(value.getKey().keySet().iterator().next(), value.getKey().values().iterator().next());
                }
            }
        }
        return result;
    }

    private int getLength(List<String> inputs, Map<String, CandidateKey> result, int length, CandidateKey key,
                          LevenshteinDistanceStrategy strategy, Map<Map<String,CandidateKey>,Double> scoredValues) {
        if (allFiles != null && key != null && allFiles.get(key.getTable()) != null ) {
            for (Map<String, String> line : allFiles.get(key.getTable())) {
                String lineValue = line.get(key.getColumn());
                int lineLength;
                if (lineValue != null) {
                    lineLength = lineValue.length();
                    length = getLengthOfRelatedFields(inputs, result, length, key, strategy, lineValue, lineLength, scoredValues);

                }
            }
        }
        return length;
    }

    private int getLengthOfRelatedFields(List<String> inputs, Map<String, CandidateKey> result, int length, CandidateKey key,
                                         LevenshteinDistanceStrategy strategy, String lineValue, int lineLength,
                                         Map<Map<String,CandidateKey>,Double>  scoredValues) {
        for (String input : inputs) {
            Double score = strategy.score(lineValue, input);
            Map<String,CandidateKey> value = new HashMap<>();
            value.put(lineValue, key);
            int inputLength = input.length();
            //TODO Add splitting the words and check if it is a substring one another it should be more than 4 chars
            // and at least two substring should contain
            if (!lineValue.isEmpty()) {
                if (inputLength > lineLength && input.contains(lineValue) && (inputLength > length)) {
                    length = lineLength;
                    result.put(lineValue, key);
                } else if ((lineLength > inputLength && lineValue.contains(input) && (lineLength > length))
                        || (inputLength == lineLength && lineValue.contains(input) && (lineLength > length))) {
                    length = inputLength;
                    result.put(lineValue, key);
                } else if (score > editDistance) {
                    scoredValues.put(value,score);
                }
            }
        }
        return length;
    }

    public List<FunctionalDependency> findFDs(Table table) throws Exception {
        TANEjava tane = new TANEjava(table, FD_ERROR);
        tane.setStoplevel(2);
        return tane.getFD();
    }

    public Set<CandidateKey> findCandidateKeys() throws Exception {
        Set<CandidateKey> result = new HashSet<>();
        List<FunctionalDependency> fd;
        KeyFinder KF;
        Set<String> candidateKeysSet;
        for(Table table : tableList) {
            if(table.getNumCols() < 10) {
                List<String> attributeList = new GapList<>();
                String tableName = table.getTableName();
                for (int i = 0; i < table.getNumCols(); i++) {
                    attributeList.add(String.valueOf(i));
                }
                KF = new KeyFinder();
                KF.setAttributeList(attributeList);
                fd = findFDs(table);
                KF.addFD(fd);
                candidateKeysSet = getLongestCompositeCandidateKey(KF.getCandidateKeys());
                for(String candidateKey : candidateKeysSet) {
                    for (Character C : candidateKey.toCharArray()) {
                        result.add(new CandidateKey(tableName, tableName + "-" + C.toString()));
                    }
               }
                KF.reset();
            } else {
                result.addAll(findCandidateKeys(table));
            }
        }
        return result;
    }

    private Set<CandidateKey> findCandidateKeys(Table table) {
       String tableName = table.getTableName();
        Set<CandidateKey> frontKeys = new HashSet<>();
            for (int i = 0; i < table.getNumCols(); i++) {
                frontKeys.add(new CandidateKey(tableName,tableName + "-" + i));
            }
        return frontKeys;
    }

    private static Set<String> getLongestCompositeCandidateKey(Set<String> candidateKeySet) {
        Set<String> result = new HashSet<>();
        String longestCandidateKey = Collections.max(candidateKeySet, Comparator.comparing(String::length));
        for(String candidateKey : candidateKeySet) {
             if (longestCandidateKey.length() == candidateKey.length()) {
               result.add(candidateKey);
            }
        }
        return result;
    }

}
