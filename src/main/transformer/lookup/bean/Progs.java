package main.transformer.lookup.bean;

import main.transformer.model.LookupSource;
import org.apache.log4j.Logger;
import org.magicwerk.brownies.collections.GapList;
import scala.Function1;

import java.util.*;
import java.util.stream.Collectors;

import static main.transformer.lookup.bean.DataStructure.getLongest;


public class Progs implements Comparable<Progs> {

    private LinkedList<Node> nodes;
    private List<CandidateKey> reachedKeys = new LinkedList<>();
    private static final Logger logger = Logger.getLogger(Progs.class);
    boolean multipleResponseWithoutFiltering = false;

    private List<Function1<List<String>, String>> howToTransformTheInput = new GapList<>();
    private Optional<Function1<List<String>, String>> howToTransformTheOutput = Optional.empty();
    private int howToTransformTheOutputScore = 0;

    public List<Node> getNodes() {
        return nodes;
    }

    public int getScore() {
        return howToTransformTheOutputScore;
    }


    void setOutputTransformFunction(Function1<List<String>, String> func, int score) {
        howToTransformTheOutput = Optional.of(func);
        howToTransformTheOutputScore = score;
    }

    private void setInputTransformFunction(List<Function1<List<String>, String>> howToTransformTheInput) {
        this.howToTransformTheInput = howToTransformTheInput;
    }

    List<Function1<List<String>, String>> getInputTransformFunction() {
        return howToTransformTheInput;
    }

    Progs(Node node) {
        nodes = new LinkedList<>(Collections.singletonList(node));
        computeReached();
    }

    private Progs(LinkedList<Node> nodes) {
        this.nodes = nodes;
        computeReached();
    }

    private void computeReached() {
        for (Node node : nodes) {
            reachedKeys.add(node.getDestKey());
        }
    }

    public Progs add(Node node) {
        LinkedList<Node> newNodes = new LinkedList<>(nodes);
        newNodes.addLast(node);
        return new Progs(newNodes);
    }

    @Override
    public int compareTo(Progs other) {
        return Collections.max(nodes).getSourceField().size() - Collections.max(other.nodes).getSourceField().size();
    }

    public int Score() {
        return nodes.size() + this.howToTransformTheInput.size();
    }

    List<CandidateKey> getReachedKeys() {
        return reachedKeys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Progs chain = (Progs) o;
        return reachedKeys.equals(chain.reachedKeys);
    }

    @Override
    public int hashCode() {
        return reachedKeys.hashCode();
    }

    // return all the possible results using the input transformation
    List<String> makeSearches(List<String> input, LookupSource source, List<Function1<List<String>,
            String>> transformers, Optional<String> expectedOutput, boolean noOutputTransformation) throws Exception {

        Map<String, List<String>> data = new HashMap<>();
        List<String> results = new GapList<>();
        List<String> resultsWithoutOutputTransformation = new GapList<>();
        List<Function1<List<String>, String>> listValidTransform = new LinkedList<>();

        //may require transformation over the output
        List<Function1<List<String>, String>> listPossibleTransform = new LinkedList<>();

        Iterator<Function1<List<String>, String>> iterator = transformers.iterator();
        List<List<String>> allInput = new GapList<>();

        CandidateKey destKey = null;
        String firstNodeData = nodes.getFirst().getSourceField().iterator().next();

        for (String individualInput : input) {
            allInput.add(Collections.singletonList(individualInput));
        }

        Function1<List<String>, String> transform = null;
        transformers.remove(null);
        String result = null;
        SyntacticLookup syntacticLookup;

        do {
            if (iterator.hasNext()) {
                allInput.clear();
                // FIXME - Why?
                allInput.add(input);
                transform = iterator.next();
                allInput.add(Collections.singletonList(transform.apply(input)));
            }

            syntacticLookup = new SyntacticLookup(input, source, expectedOutput,
                    data, results, resultsWithoutOutputTransformation, listValidTransform, listPossibleTransform,
                    allInput, destKey, firstNodeData, transform, result).invoke();
            destKey = syntacticLookup.getDestKey();
            result = syntacticLookup.getResult();

        } while (iterator.hasNext());
        if (!listValidTransform.isEmpty()) {
            setInputTransformFunction(listValidTransform);
        } else if (!listPossibleTransform.isEmpty()) {
            logger.info("use transformation that may require out transformation");
            setInputTransformFunction(listPossibleTransform);
            String output = expectedOutput.get();
            boolean contains = isContains(input, resultsWithoutOutputTransformation, output);
            if (contains) {
                results = resultsWithoutOutputTransformation;
            }
        }

        if (this.howToTransformTheOutput.isPresent() && !noOutputTransformation) {
            logger.info("transform the output using howToTransformTheOutput");
            results = Collections.singletonList(this.howToTransformTheOutput.get().apply(results));
        }
        return results;
    }

    private boolean isContains(List<String> input, List<String> resultsWithoutOutputTransformation, String output) {
        boolean contains = false;
        for (String resultWithoutOutputTransformation : resultsWithoutOutputTransformation) {
            if (resultWithoutOutputTransformation.toLowerCase().contains(output.toLowerCase()) || output.toLowerCase().contains(resultWithoutOutputTransformation.toLowerCase())) {
                contains = true;
                break;
            } else {
                for (String inputValue : input) {
                    if (inputValue.toLowerCase().contains(resultWithoutOutputTransformation.toLowerCase()) || resultWithoutOutputTransformation.toLowerCase().contains(inputValue.toLowerCase())) {
                        contains = true;
                        break;
                    }
                }
            }
        }
        return contains;
    }

    private class SyntacticLookup {
        private List<String> input;
        private LookupSource source;
        private Optional<String> expectedOutput;
        private Map<String, List<String>> data;
        private List<String> results;
        private List<String> resultsWithoutOutputTransformation;
        private List<Function1<List<String>, String>> listValidTransform;
        private List<Function1<List<String>, String>> listPossibleTransform;
        private List<List<String>> allInput;
        private CandidateKey destKey;
        private String firstNodeData;
        private Function1<List<String>, String> transform;
        private String result;

        public SyntacticLookup(List<String> input, LookupSource source, Optional<String> expectedOutput, Map<String, List<String>> data, List<String> results, List<String> resultsWithoutOutputTransformation, List<Function1<List<String>, String>> listValidTransform, List<Function1<List<String>, String>> listPossibleTransform, List<List<String>> allInput, CandidateKey destKey, String firstNodeData, Function1<List<String>, String> transform, String result) {
            this.input = input;
            this.source = source;
            this.expectedOutput = expectedOutput;
            this.data = data;
            this.results = results;
            this.resultsWithoutOutputTransformation = resultsWithoutOutputTransformation;
            this.listValidTransform = listValidTransform;
            this.listPossibleTransform = listPossibleTransform;
            this.allInput = allInput;
            this.destKey = destKey;
            this.firstNodeData = firstNodeData;
            this.transform = transform;
            this.result = result;
        }

        public CandidateKey getDestKey() {
            return destKey;
        }

        public String getResult() {
            return result;
        }

        public SyntacticLookup invoke() throws Exception {
            for (List<String> singleInput : allInput) {
                data.put(firstNodeData, singleInput);

                for (Node node : nodes) {
                    destKey = node.getDestKey();
                    List<String> values = source.queryAllSolution(node.getSourceField(), data, destKey);
                    if (!values.isEmpty()) {
                            data.put(destKey.getColumn(), values);
                    }
                }

                //no filtering over value presents twice.
                assert destKey != null;
                String mainKey = destKey.getColumn();
                List<String> dataValues = data.get(mainKey);

                if ((dataValues != null &&!dataValues.isEmpty() && !dataValues.contains(null))) {
                    if(dataValues.size() == 1 && dataValues.contains("")) {
                        result = null;
                    } else {
                        List<String> resultsWithoutDuplication = dataValues.stream().distinct().collect(Collectors.toList());
                        checkDistinctResult(singleInput, mainKey, resultsWithoutDuplication);
                    }
                } else {
                    result = null;
                }
                if (result != null && expectedOutput.isPresent() && singleInput != input) {
                    addResultIntoList();
                } else {
                    if (result != null) {
                        results.add(result);
                    }
                }
            }
            return this;
        }

        private void addResultIntoList() {
            if (expectedOutput.get().equalsIgnoreCase(result)) {
                logger.info("Valid Transformation => " + transform);
                listValidTransform.add(transform);
                results.add(result);
            } else {
                if (!result.isEmpty()) {
                    if (transform != null)
                        listPossibleTransform.add(transform);
                    if (!resultsWithoutOutputTransformation.contains(result)) {
                        resultsWithoutOutputTransformation.add(result);
                    }
                }
            }
        }

        private void checkDistinctResult(List<String> singleInput, String mainKey, List<String> resultsWithoutDuplication) {
            List<String> listKeyToFilter;
            String longestString;
            if (singleInput.size() > 1 ) {
                longestString = getLongestString(singleInput.toArray());
            } else {
                longestString = singleInput.get(0);
            }
            if (resultsWithoutDuplication.size() > 1) {
                multipleResponseWithoutFiltering = input.size() == 1;
                listKeyToFilter = data.entrySet().stream().filter(content -> !content.getKey().equalsIgnoreCase(mainKey) && content.getValue().size() > 1)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                if (!listKeyToFilter.isEmpty()) {
                    List<String> inputToFilters = input.stream().filter(p -> !p.equalsIgnoreCase(longestString)).collect(Collectors.toList());

                    int indexValidContent = 0;
                    // FIXME random candidate key selection
                    indexValidContent = getIndexValidContent(inputToFilters, indexValidContent, listKeyToFilter.get(0));
                    result = data.get(mainKey).get(indexValidContent);
                }
            } else {
                result = resultsWithoutDuplication.get(0);
            }
            resultsWithoutDuplication.clear();
        }

        private int getIndexValidContent(List<String> inputToFilters, int indexValidContent, String keyToFilter) {
            boolean isFound = false;
            for (String contentToFilter : data.get(keyToFilter)) {
                for (String inputToFilter : inputToFilters) {
                    if (inputToFilter.toLowerCase().contains(contentToFilter.toLowerCase()) ||
                            contentToFilter.toLowerCase().contains(inputToFilter.toLowerCase())) {
                        indexValidContent = data.get(keyToFilter).indexOf(contentToFilter);
                        isFound = true;
                        break;
                    }
                }
                if (isFound) {
                    break;
                }
            }
            inputToFilters.clear();
            return indexValidContent;
        }

        private String getLongestString(Object[] inputs) {
            return getLongest(inputs);
        }
    }
}