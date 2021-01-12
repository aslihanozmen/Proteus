package main.transformer.lookup.bean;

import ch.epfl.lara.synthesis.stringsolver.StringSolver;
import main.transformer.lookup.LookupTransformation;
import main.transformer.model.LookupSource;
import org.apache.log4j.Logger;
import org.magicwerk.brownies.collections.GapList;
import org.springframework.util.StopWatch;
import scala.Function1;
import services.SyntaxicMatch;
import services.SyntaxicMatch$;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static main.dataXFormerDriver.groundTruthTransformer;

public class DataStructure implements Cloneable {

    private Set<Progs> searches = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final LookupSource source;
    private Set<Progs> joinedSearches;
    private static final Logger logger = Logger.getLogger(DataStructure.class);
    private Optional<List<String>> firstInput = Optional.empty();
    private Optional<String> firstOutput = Optional.empty();

    private Optional<MergedProgs> mergedProgs = Optional.empty();
    private List<MergedProgs> listMergedPros = new GapList<>();
    private Set<CandidateKey> allReachedKeys = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Map<String, String> groundValues = new ConcurrentHashMap<>();
    private Progs wantedProgs;
    private MergedProgs wantedMergedProgs;

    public DataStructure createClone(Set<Progs> searches, Set<Progs> joinedSearches, Set<CandidateKey> allReachedKeys,
                                     Optional<MergedProgs> mergedProgs, List<MergedProgs> listMergedPros) throws CloneNotSupportedException {
        DataStructure cloned = (DataStructure) super.clone();
        cloned.setSearches(clone(searches));
        cloned.setJoinedSearches(clone(joinedSearches));
        cloned.setAllReachedKeys(clone(allReachedKeys));
        cloned.setMergedProgs(mergedProgs);
        cloned.setListMergedPros(listMergedPros);
        return cloned;
    }


    public MergedProgs getWantedMergedProgs() {
        return wantedMergedProgs;
    }

    public void setWantedMergedProgs(MergedProgs wantedMergedProgs) {
        this.wantedMergedProgs = wantedMergedProgs;
    }

    public Map<String, String> getGroundValues() {
        for (String[] value : groundTruthTransformer) {
            groundValues.put(value[1].toLowerCase(), value[0].toLowerCase());
        }
        return groundValues;
    }

    public void setGroundValues(Map<String, String> groundValues) {
        this.groundValues = groundValues;
    }

    public Set<Progs> getSearches() {
        return searches;
    }

    public Set<Progs> getJoinedSearches() {
        return joinedSearches;
    }

    public Set<CandidateKey> getAllReachedKeys() {
        return allReachedKeys;
    }

    public Optional<MergedProgs> getMergedProgs() {
        return mergedProgs;
    }

    public List<MergedProgs> getListMergedPros() {
        return listMergedPros;
    }

    public void setListMergedPros(List<MergedProgs> listMergedPros) {
        this.listMergedPros = listMergedPros;
    }

    public void setMergedProgs(Optional<MergedProgs> mergedProgs) {
        this.mergedProgs = mergedProgs;
    }

    public void setSearches(Set<Progs> searches) {
        this.searches = searches;
    }

    public void setJoinedSearches(Set<Progs> joinedSearches) {
        this.joinedSearches = joinedSearches;
    }

    public void setAllReachedKeys(Set<CandidateKey> allReachedKeys) {
        this.allReachedKeys = allReachedKeys;
    }

    public static <T> Set<T> clone(Set<T> original) {
        return new HashSet<>(original);
    }

    static class MergedProgs implements Comparable<MergedProgs> {
        private List<Progs> progs;
        private Function1<List<String>, String> transformer;

        @Override
        public int compareTo(MergedProgs other) {
            if (this.progs.size() < other.progs.size()) {
                return 1;
            } else {
                return this.progs.stream().map(Progs::Score).reduce(0, Integer::sum).compareTo(
                        other.progs.stream().map(Progs::Score).reduce(0, Integer::sum));
            }
        }

        MergedProgs(List<Progs> progs, Function1<List<String>, String> transformer) {
            this.progs = progs;
            this.transformer = transformer;
        }

        List<String> makeSearches(List<String> input, LookupSource source) throws Exception {
            logger.info("makeSearches in makeSearches in DataStructure");
            List<String> inputsForSyntactic = new GapList<>();
            logger.info("Progs: " + progs.toString());
            String longest;
            for (Progs prog : progs) {
                List<String> result = prog.makeSearches(input, source, prog.getInputTransformFunction(), Optional.empty(), true);
                logger.info("Result: " + result.toString());
                if (!result.isEmpty()) {
                    if (result.size() > 1) {
                        longest = getLongestString(result.toArray());
                    } else {
                        longest = result.get(0);
                    }
                    inputsForSyntactic.add(longest);
                }
            }
            if (input.size() > 1) {
                longest = getLongestString(input.toArray());
            } else {
                longest = input.get(0);
            }
            inputsForSyntactic.add(longest);
            return Collections.singletonList(transformer.apply(inputsForSyntactic));
        }

        private static String getLongestString(Object[] inputs) {
            return getLongest(inputs);
        }
    }

    static String getLongest(Object[] inputs) {
        int largestString = inputs[0].toString().length();
        int index = 0;
        for (int i = 0; i < inputs.length; i++) {
            if (inputs[i].toString().length() > largestString) {
                largestString = inputs[i].toString().length();
                index = i;
            }
        }
        return inputs[index].toString();
    }


    public boolean containsSolution() {
        return searches != null && !searches.isEmpty();
    }

    // use searches to test if one of them match the new request
    public boolean filterSolutions(List<String> input, String output) throws Exception {
        //use actual first input/output to search for all progs to find a match
        List<List<Progs>> validProgs = new GapList<>();
        List<String> inputsForSyntactic = new GapList<>();
        List<String> firstInputExample;
        List<String> result;
        Function1<List<String>, String> ourTransformerInputs;
        String firstElementInInputExample;
        String firstOutputExample;
        if (firstInput.isPresent() && firstOutput.isPresent()) {
            firstInputExample = firstInput.get();
            firstElementInInputExample = firstInputExample.get(0);
            firstOutputExample = firstOutput.get();
        } else {
            throw new IllegalArgumentException("Input and output cannot be null!");
        }
        inputsForSyntactic.add(firstElementInInputExample);

        List<SyntaxicMatch.TransformMultipleInputs> transform = new GapList<>();
        Set<Progs> subsetSearch = new HashSet<>(searches);

        //filter the prog1 that does not generate a value close to the output
        List<Progs> rejectedProgs = new GapList<>();

        //sorting
        List<Progs> progsSearches = searches.stream().sorted().collect(Collectors.toList());
        logger.info("#Progs " + progsSearches.size());
        filterSearches(validProgs, firstInputExample, firstElementInInputExample, firstOutputExample, transform,
                subsetSearch, rejectedProgs, progsSearches);
        boolean weHaveASolution = false;
        for (List<Progs> progs : validProgs) {
            ourTransformerInputs = transform.get(validProgs.indexOf(progs)).how();
            inputsForSyntactic = new GapList<>();
            String longest;
            for (Progs prog : progs) {
                result = prog.makeSearches(input, source, prog.getInputTransformFunction(), Optional.empty(), true);
                if (!result.isEmpty()) {
                    if (result.size() > 1) {
                        longest = getLongest(result.toArray());
                    } else {
                        longest = result.get(0);
                    }
                    inputsForSyntactic.add(longest);
                } else {
                    searches.remove(prog);
                    joinedSearches.remove(prog);
                }
            }
            if (input.size() > 1) {
                longest = getLongest(input.toArray());
            } else {
                longest = input.get(0);
            }
            inputsForSyntactic.add(longest);
            if (ourTransformerInputs.apply(inputsForSyntactic).equalsIgnoreCase(output)) {
                mergedProgs = Optional.of(new MergedProgs(progs, ourTransformerInputs));
                listMergedPros.add(new MergedProgs(progs, ourTransformerInputs));
                weHaveASolution = true;
            }
        }
        if (!weHaveASolution) {
            //no solution - attempt to use the actual input/output as original input/output
            firstOutput = Optional.of(output);
            firstInput = Optional.of(input);
        }
        validProgs.clear();
        inputsForSyntactic.clear();
        return weHaveASolution;
    }

    private void filterSearches(List<List<Progs>> validProgs, List<String> firstInputExample, String firstElementInInputExample,
                                String firstOutputExample, List<SyntaxicMatch.TransformMultipleInputs> transform,
                                Set<Progs> subsetSearch, List<Progs> rejectedProgs, List<Progs> progsSearches) throws Exception {
        List<String> inputsForSyntactic;
        boolean isRestChecked = false;
        String longest;
        for (Progs prog1 : progsSearches) {

            List<Progs> tempValidProgs = new GapList<>();
            subsetSearch.remove(prog1);
            subsetSearch.removeAll(rejectedProgs);

            if (rejectedProgs.contains(prog1) || prog1 == null) {
                searches.remove(prog1);
                joinedSearches.remove(prog1);
            } else {
                List<String> firstResponse = prog1.makeSearches(firstInputExample, source, prog1.getInputTransformFunction(), Optional.empty(), true);

                if ((SyntaxicMatch$.MODULE$.score(firstResponse, firstOutputExample) < 2) || firstResponse.isEmpty()) {
                    searches.remove(prog1);
                    joinedSearches.remove(prog1);
                } else {
                    if (firstResponse.size() > 1) {
                        longest = getLongest(firstResponse.toArray());
                        logger.info("Result (firstResponse): " + firstResponse);
                    } else {
                        longest = firstResponse.get(0);
                        logger.info("Result (firstResponse): " + firstResponse);
                    }
                    inputsForSyntactic = Arrays.asList(longest, firstElementInInputExample);
                    validProgs.add(Collections.singletonList(prog1));

                    checkSyntacticScore(firstOutputExample, transform, inputsForSyntactic);

                    logger.info("#Progs (subsetSearch): " + subsetSearch.size());
                    isRestChecked = isRestChecked(validProgs, firstInputExample, firstElementInInputExample, firstOutputExample, transform, subsetSearch, rejectedProgs, progsSearches, isRestChecked, prog1, tempValidProgs, longest);
                    if (isRestChecked) {
                        break;
                    }
                }
            }
        }
    }

    private void checkSyntacticScore(String firstOutputExample, List<SyntaxicMatch.TransformMultipleInputs> transform, List<String> inputsForSyntactic) {
        if (SyntaxicMatch$.MODULE$.score(inputsForSyntactic, firstOutputExample) > 15) {
            transform.add(SyntaxicMatch$.MODULE$.transformMultipleInputs(inputsForSyntactic, firstOutputExample));
        } else {
            transform.add(SyntaxicMatch$.MODULE$.unitTransformInput());
        }
    }

    private boolean isRestChecked(List<List<Progs>> validProgs, List<String> firstInputExample, String firstElementInInputExample,
                                  String firstOutputExample, List<SyntaxicMatch.TransformMultipleInputs> transform,
                                  Set<Progs> subsetSearch, List<Progs> rejectedProgs, List<Progs> progsSearches,
                                  boolean isRestChecked, Progs prog1, List<Progs> tempValidProgs,
                                  String firstElementInResponse) throws Exception {
        List<String> inputsForSyntactic;
        String longest;
        for (Progs prog2 : subsetSearch) {
            if (prog2 == prog1 || rejectedProgs.contains(prog1)) {
                searches.remove(prog2);
                joinedSearches.remove(prog2);
                progsSearches.remove(prog2);
                continue;
            } else {
                List<String> result = prog2.makeSearches(firstInputExample, source, prog2.getInputTransformFunction(), Optional.empty(), true);
                if (result.isEmpty()) {
                    searches.remove(prog2);
                    joinedSearches.remove(prog2);
                    progsSearches.remove(prog2);
                    continue;
                } else if (SyntaxicMatch$.MODULE$.score(result, firstOutputExample) < 2) {
                    rejectedProgs.add(prog2);
                    searches.remove(prog2);
                    joinedSearches.remove(prog2);
                    progsSearches.remove(prog2);
                } else {
                    if (result.size() > 1) {
                        logger.info("Result: " + result);
                        longest = getLongest(result.toArray());
                    } else {
                        logger.info("Result: " + result);
                        longest = result.get(0);
                    }
                    inputsForSyntactic = Arrays.asList(firstElementInResponse, longest, firstElementInInputExample);
                    tempValidProgs.add(prog1);
                    tempValidProgs.add(prog2);
                    validProgs.add(tempValidProgs);
                    tempValidProgs = new GapList<>();
                    checkSyntacticScore(firstOutputExample, transform, inputsForSyntactic);
                }
            }
            isRestChecked = true;
        }
        return isRestChecked;
    }

    public void setFirstInput(List<String> inputs) {
        firstInput = Optional.ofNullable(inputs);
    }

    public void setFirstOutput(String output) {
        firstOutput = Optional.ofNullable(output);
    }

    private ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public DataStructure(LookupSource source) {
        this.source = source;
    }

    public Set<Progs> generateStrWithCandidateKeys(Set<CandidateKey> dataKeys, Set<CandidateKey>  candidateKeys) {
        joinedSearches = Collections.newSetFromMap(new ConcurrentHashMap<>());
        // Set<CandidateKey> candidateKeys = LookupTransformation.getCandidateKeys();
        GapList<String> candidates = new GapList<>();
        for(CandidateKey ck : candidateKeys) {
            candidates.add(ck.getColumn());
        }
        for (CandidateKey inputKey : dataKeys) {
            candidates.remove(inputKey.getColumn());
            GapList<String> inter = new GapList((candidates));
            // temporary - reversing resolve the issue with S30
            recursiveChain(Collections.singleton(inputKey), inter, null);
            Collections.reverse(inter);
            recursiveChain(Collections.singleton(inputKey), inter, null);
            searches.addAll(joinedSearches);
        }
        joinedSearches.clear();
        searches.addAll(joinedSearches);
        return searches;
    }



    public Set<Progs> generateStr(Set<CandidateKey> dataKeys) throws Exception {
        joinedSearches = Collections.newSetFromMap(new ConcurrentHashMap<>());
        for (CandidateKey inputKey : dataKeys) {
            Collection<String> candidates = new HashSet<>(source.getColumnNames(inputKey.getTable()));
            candidates.remove(inputKey.getColumn());
            GapList<String> inter = new GapList((candidates));
            // temporary - reversing resolve the issue with S30
            recursiveChain(Collections.singleton(inputKey), inter, null);
            Collections.reverse(inter);
            recursiveChain(Collections.singleton(inputKey), inter, null);
            searches.addAll(joinedSearches);
        }
        joinedSearches.clear();
        Set<String> tables = new HashSet<>(source.getTables());
        Collection<Callable<Object>> list = new LinkedList<>();
        for (Progs chain : searches) {
            List<CandidateKey> reachedKeys = chain.getReachedKeys();
            for (String nextTable : tables) {
                if (!reachedKeys.stream().map(CandidateKey::getTable).collect(Collectors.toSet()).contains(nextTable)) {
                    Collection<String> nextColumns = new HashSet<>(source.getColumnNames(nextTable));
                    Set<CandidateKey> nextKeys = reachedKeys.stream()
                            .map(key -> new CandidateKey(nextTable, key.getColumn()))
                            .collect(Collectors.toSet());
                    list.add(() -> {
                        recursiveChain(nextKeys, nextColumns, chain);
                        return Collections.emptyList();
                    });
                }
            }
        }
        executor.invokeAll(list);
        searches.addAll(joinedSearches);
        tables.clear();
        list.clear();
        return searches;
    }

    private void recursiveChain(Set<CandidateKey> keys, Collection<String> candidates, Progs chain) {
        for (String dest : candidates) {
            String table = keys.iterator().next().getTable();
            Set<String> srcColumns = keys.stream().map(CandidateKey::getColumn).collect(Collectors.toSet());

            if (chain == null) {
                chain = new Progs(new Node(table, srcColumns, dest));
            } else {
                chain = chain.add(new Node(table, srcColumns, dest));
            }

            if (!searches.contains(chain)) {
                joinedSearches.add(chain);
            }
        }
    }

    public StringSolver intersect(List<String> inputs, String expectedOutput, List<Function1<List<String>, String>> transformers) throws InterruptedException {
        StopWatch sWatchSearch = new StopWatch("intersect");
        sWatchSearch.start("intersect");
        List<Callable<String>> calls = searches.stream().map(progs -> (Callable<String>) () -> {

            List<String> results = progs.makeSearches(inputs, source, transformers, Optional.of(expectedOutput), false);

            results.addAll(inputs);
            String returnedResult = null;
            for (String result : results) {
                if (transformInput(inputs, expectedOutput, transformers, progs, results, result)) return result;
                returnedResult = result;
            }
            results.clear();
            return returnedResult;
        }).collect(toList());
        executor.invokeAll(calls);
        sWatchSearch.stop();
        logger.info("Intersection is started");
        logger.info(sWatchSearch.prettyPrint());
        calls.clear();
        return null;
    }

    private boolean transformInput(List<String> inputs, String expectedOutput, List<Function1<List<String>, String>> transformers, Progs progs, List<String> results, String result) {
        if (result.toLowerCase().contains(expectedOutput.toLowerCase()) || expectedOutput.toLowerCase().contains(result.toLowerCase())) {
            StopWatch sWatch = new StopWatch("PROGS- thread id:" + Thread.currentThread().getName());
            sWatch.start("transform operation");
            SyntaxicMatch.TransformMultipleInputs interOutputTransformer = SyntaxicMatch$.MODULE$.transformMultipleInputs(results, expectedOutput);
            progs.setOutputTransformFunction(interOutputTransformer.how(), interOutputTransformer.score());
            sWatch.stop();
            applyTransformation(inputs, expectedOutput, transformers, result);
            return true;
        }
        return false;
    }

    private void applyTransformation(List<String> inputs, String expectedOutput, List<Function1<List<String>, String>> transformers, String result) {
        //IgnoreCase Fix
        if (!expectedOutput.equalsIgnoreCase(result)) {
            for (Function1<List<String>, String> transform : transformers) {
                transform.apply(inputs);
            }
        }
        transformers.clear();
    }

    public String findData(List<String> input, boolean userInteraction) throws Exception {
        List<Progs> progSorted;
        List<String> result;
        String longest = null;
        boolean shouldContinue = true;


        if (!userInteraction) {
            String longestWithMergedProgs = findDataWithMergedProgs(input);
            if (longestWithMergedProgs != null) return longestWithMergedProgs;

            Progs max;
            while (shouldContinue) {
                shouldContinue = false;
                if (!searches.isEmpty()) {
                    max = Collections.max(searches);

                    progSorted = searches.stream()
                            .sorted(Comparator.comparingInt(Progs::getScore).reversed())
                            .collect(Collectors.toList());

                    for (Progs prog : progSorted) {
                        prog.makeSearches(input, source, max.getInputTransformFunction(), Optional.empty(), false);
                        if (!prog.multipleResponseWithoutFiltering) {
                            max = prog;
                            break;
                        }
                    }
                    result = max.makeSearches(input, source, max.getInputTransformFunction(), Optional.empty(), false);
                    if (result.size() > 1) {
                        longest = getLongest(result.toArray());
                    } else if ( !result.isEmpty() && !result.get(0).equals("")) {
                        longest = result.get(0);
                    } else {
                        searches.remove(max);
                        shouldContinue = true;
                    }
                }
            }

        } else {
            logger.info("findData - using mergedProgs");
            List<String> searchResults;
            Map<String, MergedProgs> resultsWithMergedProgs = new ConcurrentHashMap<>();

            if (!listMergedPros.isEmpty()) {
                //test with list
                List<MergedProgs> sortedMergedProgs = listMergedPros.stream()
                        .sorted(Comparator.reverseOrder())
                        .collect(Collectors.toList());

                for (MergedProgs progs : sortedMergedProgs) {
                    String longestInput;
                    searchResults = progs.makeSearches(input, source);
                    if (searchResults.size() > 1) {
                        longest = getLongest(searchResults.toArray());
                    } else {
                        longest = searchResults.get(0);
                    }
                    if (input.size() > 1) {
                        longestInput = getLongest(input.toArray());
                    } else {
                        longestInput = input.get(0);
                    }
                    if (!longest.equals("") && !longest.equalsIgnoreCase(longestInput)) {
                        resultsWithMergedProgs.put(longest, progs);
                    }
                }
                for (Map.Entry<String, MergedProgs> progResult : resultsWithMergedProgs.entrySet()) {
                    if (getGroundValues().containsKey(progResult.getKey().toLowerCase())) {
                        wantedMergedProgs = progResult.getValue();
                        break;
                    }
                }
                logger.info("noMatch");
                sortedMergedProgs.clear();
            }


            if (resultsWithMergedProgs.isEmpty()) {
                longest = null;
                Map<Progs, String> results = new ConcurrentHashMap<>();

                for (Progs max : searches) {
                    progSorted = searches.stream()
                            .sorted(Comparator.comparingInt(Progs::getScore).reversed())
                            .collect(Collectors.toList());

                    for (Progs prog : progSorted) {
                        prog.makeSearches(input, source, max.getInputTransformFunction(), Optional.empty(), false);
                        if (!prog.multipleResponseWithoutFiltering) {
                            max = prog;
                            break;
                        }
                    }
                    result = max.makeSearches(input, source, max.getInputTransformFunction(), Optional.empty(), false);
                    if (result.size() > 1) {
                        longest = getLongest(result.toArray());
                        results.put(max, longest);
                    } else if (!result.isEmpty() && !result.get(0).equals("")) {
                        longest = result.get(0);
                        results.put(max, longest);
                    }
                }
                for (Map.Entry<Progs, String> progResult : results.entrySet()) {
                    if (getGroundValues().containsKey(progResult.getValue().toLowerCase())) {
                        wantedProgs = progResult.getKey();
                        break;
                    }
                }
            }

        }
        return longest;

    }

    public String findRealData(List<String> input) throws Exception {

        String longestForMergedProgs;
        String longestInput;
        if (wantedMergedProgs != null) {
            List<String> searchResults = wantedMergedProgs.makeSearches(input, source);
            if (searchResults.size() > 1) {
                longestForMergedProgs = getLongest(searchResults.toArray());
            } else {
                longestForMergedProgs = searchResults.get(0);
            }
            if (input.size() > 1) {
                longestInput = getLongest(input.toArray());
            } else {
                longestInput = input.get(0);
            }
            if (!longestForMergedProgs.equals("") && !longestForMergedProgs.equalsIgnoreCase(longestInput)) {
                return longestForMergedProgs;
            }
            return longestForMergedProgs;
        }


        if (wantedProgs != null) {
            String longest = null;
            List<String> result = wantedProgs.makeSearches(input, source, wantedProgs.getInputTransformFunction(), Optional.empty(), false);
            if (result.size() > 1) {
                longest = getLongest(result.toArray());
            } else if (!result.isEmpty() && !result.get(0).equals("")) {
                longest = result.get(0);
            }
            return longest;
        }
        return null;
    }

    private String findDataWithMergedProgs(List<String> input) throws Exception {
        logger.info("findData - using mergedProgs");
        List<String> searchResults;

        if (!listMergedPros.isEmpty()) {
            //test with list
            List<MergedProgs> sortedMergedProgs = listMergedPros.stream()
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());

            for (MergedProgs progs : sortedMergedProgs) {
                String longest;
                String longestInput;
                searchResults = progs.makeSearches(input, source);
                if (searchResults.size() > 1) {
                    longest = getLongest(searchResults.toArray());
                } else {
                    longest = searchResults.get(0);
                }
                if (input.size() > 1) {
                    longestInput = getLongest(input.toArray());
                } else {
                    longestInput = input.get(0);
                }
                if (!longest.equals("") && !longest.equalsIgnoreCase(longestInput)) {
                    return longest;
                }
            }
            logger.info("noMatch");
            sortedMergedProgs.clear();
        }
        return null;
    }
}
