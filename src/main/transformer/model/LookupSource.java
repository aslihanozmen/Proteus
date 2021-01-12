package main.transformer.model;

import main.transformer.lookup.bean.CandidateKey;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface LookupSource {

    Collection<String> getColumnNames(String tableName) throws Exception;

    Collection<String> getTables() throws Exception;

    Set<CandidateKey> findDataKeys(Set<CandidateKey> keys, List<String> input) throws Exception;

    Set<CandidateKey> findCandidateKeys() throws Exception;

    String query(Set<String> srcKey, Map<String, List<String>> param, CandidateKey destKey) throws Exception;

    Map<String, CandidateKey> allDataKeys(Set<CandidateKey> keys, List<String> inputs) throws Exception;

    Map<String, String> findAll(String input, String table)throws Exception;

    List<String> queryAllSolution(Set<String> srcKey, Map<String, List<String>> param, CandidateKey destKey)throws Exception;
}
