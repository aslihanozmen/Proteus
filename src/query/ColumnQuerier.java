package query;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import main.WTTransformerMultiColSets;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.BooleanClause.Occur;

import util.StemHistogram;
import util.StemMap;

/**
 * @author John Morcos
 * This class takes the set of {(x, f(x))} U {(x, - )} and issues queries to find tables
 */
public class ColumnQuerier {

    public final float LUCENE_SIMILARITY_THRESHOLD = 0.8f;

    private String keywords;
    private String header1;
    private String header2;


    //Init index
    IndexReader ir = null;
    IndexSearcher is = null;
    ComplexPhraseQueryParser qp = new ComplexPhraseQueryParser("contents", new StandardAnalyzer());

    public ColumnQuerier(String keywords, String header1, String header2, IndexReader ir) throws IOException {
        this.keywords = keywords;
        this.header1 = header1;
        this.header2 = header2;

        this.ir = ir;
        this.is = new IndexSearcher(this.ir);
    }

    //TODO: Add extensible matching rules for each column (e.g. queries for swift: AAAAAAAA*)...or fuzziness..etc

    /**
     * Finds ids of relevant tables, in the order to search
     *
     * @param keyToImages
     * @param knownExamples
     * @param maxNQueries
     * @return
     * @throws IOException
     * @throws ParseException
     */
    public ArrayList<String> findTables(StemMap<StemMap<HashSet<String>>> keyToImages, StemMap<StemHistogram> knownExamples, int maxNQueries) throws IOException, ParseException {
        ArrayList<String> tableIDs = new ArrayList<String>();

        List<List<String>> partitionedKeys = partitionKeys(keyToImages, knownExamples, Integer.MAX_VALUE);

        int p = 0;

        for (List<String> partition : partitionedKeys) {
            boolean boost = false;

            if (maxNQueries > -1 && p > maxNQueries) {
                break;
            }
            p++;

            BooleanQuery.Builder colFromQueryBuilder = new BooleanQuery.Builder();
            BooleanQuery.Builder contentsQueryBuilder = new BooleanQuery.Builder();

            for (String key : partition) {

                if (!knownExamples.containsKey(key)) {
                    continue;
                } else {
                    //boost = true;
                }

                String s2 = "";
                StringReader sr = new StringReader(key);

                int tokens = 0;
                StandardTokenizer tokenizer = new StandardTokenizer();
                tokenizer.setReader(sr);
                tokenizer.reset();

                while (tokenizer.incrementToken()) {
                    s2 += " " + tokenizer.getAttribute(CharTermAttribute.class);
                    if (LUCENE_SIMILARITY_THRESHOLD > 0) {
                        s2 += "~" + LUCENE_SIMILARITY_THRESHOLD;
                    }
                    tokens++;
                }

                if (tokens > 1) {
                    s2 = " \"" + s2 + '"';
                }

                contentsQueryBuilder.add(qp.parse("contents: " + s2), Occur.SHOULD);
                //query.append(s2);
                tokenizer.close();
                sr.close();
            }

            //query.append(")");
            contentsQueryBuilder.setMinimumNumberShouldMatch(WTTransformerMultiColSets.COVERAGE_THRESHOLD);
            colFromQueryBuilder.add(contentsQueryBuilder.build(), Occur.MUST);

            Query header1Query = null;
            if (header1 != null) {
                //query.append(" header: (" + header1 + ")");
                header1Query = qp.parse("header: (" + header1 + ")");
                colFromQueryBuilder.add(header1Query, Occur.SHOULD);
            }

            //query.append(" title: (" + keywords + ") context: (" + keywords + ")");
            Query contextQueries = qp.parse("title: (" + keywords + ") context: (" + keywords + ")");
            colFromQueryBuilder.add(contextQueries, Occur.SHOULD);


            HashSet<String> tableFromIDs = new HashSet<>();
            ArrayList<Integer> colDocs = executeQuery(colFromQueryBuilder.build(), 1000);
            for (int colDoc : colDocs) {
                tableFromIDs.add(ir.document(colDoc).get("tableId"));
            }


            //Look for the colTo
            //query = new StringBuffer();
            BooleanQuery.Builder colToQueryBuilder = new BooleanQuery.Builder();
            contentsQueryBuilder = new BooleanQuery.Builder();
            for (String key : partition) {
                String s2;
                //TODO: if functional
                for (String v : keyToImages.get(key).keySet()) {
                    s2 = v;
                    if (s2 == null) {
                        continue;
                    }

                    StringReader sr = new StringReader(s2);

                    s2 = "";

                    int tokens = 0;

                    StandardTokenizer tokenizer = new StandardTokenizer();
                    tokenizer.setReader(sr);
                    tokenizer.reset();
                    while (tokenizer.incrementToken()) {
                        s2 += " " + tokenizer.getAttribute(CharTermAttribute.class);
                        if (LUCENE_SIMILARITY_THRESHOLD > 0) {
                            s2 += "~" + LUCENE_SIMILARITY_THRESHOLD;
                        }
                        tokens++;
                    }

                    if (tokens > 1) {
                        s2 = " \"" + s2 + '"';
                    }
                    if (boost) {
                        s2 += "^10";
                    }
                    //query.append(s2);
                    contentsQueryBuilder.add(qp.parse("contents: " + s2), Occur.SHOULD).build();
                    tokenizer.close();
                    sr.close();
                }
            }

            colToQueryBuilder.add(contentsQueryBuilder.build(), Occur.MUST);
            Query header2Query = null;
            if (header2 != null) {
                header2Query = qp.parse("header: (" + header2 + ")");
                colToQueryBuilder.add(header2Query, Occur.SHOULD);
            }
            colToQueryBuilder.add(contextQueries, Occur.SHOULD);

            ArrayList<Integer> colToDocs = executeQuery(colToQueryBuilder.build(), 1000);
            for (int colDoc : colToDocs) {
                String tableId = ir.document(colDoc).get("tableId");
                if (tableFromIDs.contains(tableId)) {
                    tableIDs.add(tableId);
                }
            }

        }
        return tableIDs;
    }

    /**
     * Executes the query and gets the lucene docIDs
     *
     * @param query
     * @param topk
     * @return
     * @throws IOException
     */
    public ArrayList<Integer> executeQuery(Query query, int topk) throws IOException {
        ArrayList<Integer> docs = new ArrayList<>();
        TopDocs hits = is.search(query, topk);

        for (ScoreDoc hit : hits.scoreDocs) {
            docs.add(hit.doc);
        }

        return docs;
    }


    /**
     * Partitions the set of available known examples to get more targeted queries
     *
     * @param keyToImages
     * @param knownExamples
     * @return List of List containing keys/ partitions of keys having <= keysPerPartition elements
     */
    private List<List<String>> partitionKeys(HashMap<String, StemMap<HashSet<String>>> keyToImages, StemMap<StemHistogram> knownExamples, int keysPerPartition) {
        ArrayList<List<String>> partitions = new ArrayList<List<String>>();

        String[] keys = knownExamples.keySet().toArray(new String[0]);

        final HashMap<String, Integer> keyToSupport = new HashMap<>();

        for (String key : keyToImages.keySet()) {
            StemMap<HashSet<String>> images = keyToImages.get(key);
            HashSet<String> allSupportingTables = new HashSet<String>();
            for (HashSet<String> supportingTables : images.values()) {
                allSupportingTables.addAll(supportingTables);
            }

            keyToSupport.put(key, allSupportingTables.size());
        }

        //sort ascending (rarest are preferred first)
        Arrays.sort(keys, (s1, s2) -> (Integer.compare(keyToSupport.get(s1), keyToSupport.get(s2))));

        ArrayList<String> curPartition = new ArrayList<String>(keysPerPartition);
        int curCount = 0;
        for (String s : keys) {
            if (curCount == keysPerPartition) {
                partitions.add(curPartition);
                curPartition = new ArrayList<String>(keysPerPartition);
                curCount = 0;
            }
            curPartition.add(s);
            curCount++;
        }
        if (curCount > 0) {
            partitions.add(curPartition);
        }

        return partitions;
    }


}
