package main.transformer.lookup;

import main.transformer.lookup.bean.CandidateKey;
import main.transformer.lookup.bean.DataStructure;
import main.transformer.lookup.bean.Progs;
import main.transformer.model.LookupSource;
import org.apache.log4j.Logger;
import org.magicwerk.brownies.collections.GapList;
import scala.Function1;
import services.SyntaxicMatch;
import services.SyntaxicMatch$;

import java.util.*;

public class LookupTransformation {

    private final LookupSource source;
    private DataStructure dataStructure;
    private Set<CandidateKey> candidateKeys;

    private List<Function1<List<String>, String>> howToTransformInput = new GapList<>();
    private static final Logger logger = Logger.getLogger(LookupTransformation.class);


    public LookupTransformation(LookupSource source) {
        this.source = source;
        this.dataStructure = new DataStructure(source);
        candidateKeys = null;
    }

    public void provideMultipleInputsExample(List<String> inputs, String expectedOutput) throws Exception {
        boolean generateNewProgs = false;
        if (dataStructure.containsSolution()) {
            if (dataStructure.filterSolutions(inputs, expectedOutput)) return;
            else {
                generateNewProgs = true;
            }
        } else { //first call
            dataStructure.setFirstInput(inputs);
            dataStructure.setFirstOutput(expectedOutput);
        }

        if(candidateKeys == null || candidateKeys.isEmpty()) {
            candidateKeys = findCandidateKeys();
        }
        Set<CandidateKey> result = source.findDataKeys(candidateKeys, inputs);

        if (result.isEmpty()) {
            //<Content, key>
            Map<String, CandidateKey> allFields = source.allDataKeys(candidateKeys, inputs);
            for (String dbfield : allFields.keySet()) {
                //Add contains check here even before it goes to stringSolver between (inputs and dbfield)
                SyntaxicMatch.MatchInputsResponse matchMultipleInput = SyntaxicMatch$.MODULE$.inputsMatch(inputs, dbfield);
                logger.info(inputs.toString() + "requires syntactic manipulations");

                if (matchMultipleInput.valid()) {
                    howToTransformInput.add(matchMultipleInput.how());
                    result.add(allFields.get(dbfield));
                }
            }
            allFields.clear();
        }
        final Set<Progs> progs = dataStructure.generateStr(result);
        result.clear();
        logger.info("start - using mergedProgs");
        dataStructure.intersect(inputs, expectedOutput, howToTransformInput);

        DataStructure dataStructurePrevious = dataStructure.createClone(dataStructure.getSearches(), dataStructure.getJoinedSearches(), dataStructure.getAllReachedKeys(),
                dataStructure.getMergedProgs(), dataStructure.getListMergedPros());

        if (generateNewProgs) {
            dataStructure.filterSolutions(inputs, expectedOutput);
        }
        try {
            if (progs.isEmpty()) {
                System.err.println("no match for requested data");
                throw new RuntimeException("no match for requested data");

            }
        } catch (Exception e) {
            System.out.println("Rolling back...");
            dataStructure.setAllReachedKeys(dataStructurePrevious.getAllReachedKeys());
            dataStructure.setJoinedSearches(dataStructurePrevious.getJoinedSearches());
            dataStructure.setSearches(dataStructurePrevious.getSearches());
            dataStructure.setMergedProgs(dataStructurePrevious.getMergedProgs());
            dataStructure.setListMergedPros(dataStructurePrevious.getListMergedPros());
        }
    }

    public void provideMultipleInputsExampleWithCandidateKeys(List<String> inputs, String expectedOutput, Set<CandidateKey> providedCandidateKeys) throws Exception {
        boolean generateNewProgs = false;
        if (dataStructure.containsSolution()) {
            if (dataStructure.filterSolutions(inputs, expectedOutput)) return;
            else {
                generateNewProgs = true;
            }
        } else { //first call
            dataStructure.setFirstInput(inputs);
            dataStructure.setFirstOutput(expectedOutput);
        }
        candidateKeys = providedCandidateKeys;
        Set<CandidateKey> result = source.findDataKeys(candidateKeys, inputs);

        if (result.isEmpty()) {
            //<Content, key>
            Map<String, CandidateKey> allFields = source.allDataKeys(candidateKeys, inputs);
            for (String dbfield : allFields.keySet()) {
                //Add contains check here even before it goes to stringSolver between (inputs and dbfield)

                SyntaxicMatch.MatchInputsResponse matchMultipleInput = SyntaxicMatch$.MODULE$.inputsMatch(inputs, dbfield);
                logger.info(inputs.toString() + "requires syntactic manipulations");

                if (matchMultipleInput.valid()) {
                    howToTransformInput.add(matchMultipleInput.how());
                    result.add(allFields.get(dbfield));
                }
            }
            allFields.clear();
        }
        final Set<Progs> progs = dataStructure.generateStrWithCandidateKeys(result, candidateKeys);
        result.clear();
        logger.info("start - using mergedProgs");
        dataStructure.intersect(inputs, expectedOutput, howToTransformInput);

        DataStructure dataStructurePrevious = dataStructure.createClone(dataStructure.getSearches(), dataStructure.getJoinedSearches(), dataStructure.getAllReachedKeys(),
                dataStructure.getMergedProgs(), dataStructure.getListMergedPros());

        if (generateNewProgs) {
            dataStructure.filterSolutions(inputs, expectedOutput);
        }
        try {
            if (progs.isEmpty()) {
                System.err.println("no match for requested data");
                throw new RuntimeException("no match for requested data");

            }
        } catch (Exception e) {
            System.out.println("Rolling back...");
            dataStructure.setAllReachedKeys(dataStructurePrevious.getAllReachedKeys());
            dataStructure.setJoinedSearches(dataStructurePrevious.getJoinedSearches());
            dataStructure.setSearches(dataStructurePrevious.getSearches());
            dataStructure.setMergedProgs(dataStructurePrevious.getMergedProgs());
            dataStructure.setListMergedPros(dataStructurePrevious.getListMergedPros());
        }
    }

    public void provideExample(String input, String expectedOutput) throws Exception {
        provideMultipleInputsExample(Collections.singletonList(input), expectedOutput);
    }

    private Set<CandidateKey> findCandidateKeys() throws Exception {
        Set<CandidateKey> frontKeys = new HashSet<>();
        for (String table : source.getTables()) {
            for (String column : source.getColumnNames(table)) {
                frontKeys.add(new CandidateKey(table, column));
            }
        }
        return frontKeys;
    }

    public String findMatch(String input, boolean isUserInvolved) throws Exception {
        return findMultipleInputMatch(Collections.singletonList(input), isUserInvolved);
    }

    public String findMultipleInputMatch(List<String> input, boolean isUserInvolved) throws Exception {
        return dataStructure.findData(input, isUserInvolved);
    }

    public String findRealDataWithTransformation(List<String> inputs) throws Exception {
        return dataStructure.findRealData(inputs);
    }
}
