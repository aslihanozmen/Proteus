package query.multiColumn;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.automaton.LevenshteinAutomata;

import org.magicwerk.brownies.collections.GapList;
import query.multiColumn.AbstractMultiColVerticaQuerier.Policy;
import model.Tuple;
import model.VerticaBatchTableLoader;

import com.vertica.jdbc.VerticaConnection;

import util.Histogram;
import util.MultiColMatchCase;
import util.Pair;
import util.StemHistogram;
import util.StemMap;
import util.StemMultiColHistogram;
import util.StemMultiColMap;
import util.StringUtil;
import util.MatchCase;

public class VerticaMultiColSimilarityQuerier extends AbstractMultiColVerticaQuerier implements MultiColTableQuerier {

    //Unstemmed version, for similarity checking
    public static float maxDistance = 0.2f;
    private IndexReader indexReader = null;
    private IndexSearcher indexSearcher = null;
    public HashMap<List<String>, Collection<List<String>>> exactKnownExamples;

    public HashMap<String, Collection<String>> valToSimilarValForms;
    public StemMap<Collection<String>> valToSimilarValFormsTokenized;


    public VerticaMultiColSimilarityQuerier(VerticaConnection con, String termIndexDir) throws IOException {
        this(con, Policy.ALL, -1, -1, termIndexDir);
    }

    public VerticaMultiColSimilarityQuerier(
            VerticaConnection con, Policy policy,
            int maxExamples, int maxExamplesPerX,
            String termIndexDir) throws IOException {
        super(con, policy, maxExamples, maxExamplesPerX);
        indexReader = DirectoryReader.open(FSDirectory.open(new File(termIndexDir).toPath()));
        indexSearcher = new IndexSearcher(indexReader);
    }


    @Override
    public ArrayList<MultiColMatchCase> findTables(
            StemMultiColMap<StemMultiColHistogram> knownExamples, int examplesPerQuery) throws IOException {

        if (examplesPerQuery > knownExamples.size()) {
            for (List<String> k : knownExamples.keySet()) {
                for (List<String> v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                    System.out.println(k + " -> " + v);
                }
            }
            throw new IllegalStateException("|known examples| = " + knownExamples.size()
                    + ", tau = " + examplesPerQuery);
        }


        StemMultiColMap<StemMultiColHistogram> selectedExamples = null;

        try {
            selectedExamples = selectExamples(knownExamples);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (exactKnownExamples == null) {
            throw new IllegalStateException("Exact examples must be set before using the VerticaSimilarityQuerier.");
        }
        if (valToSimilarValFormsTokenized == null) {
            valToSimilarValForms = new HashMap<String, Collection<String>>();

            valToSimilarValFormsTokenized = new StemMap<Collection<String>>();

            for (List<String> exactKey : exactKnownExamples.keySet()) {
                for (String k : exactKey) {
                    if (!valToSimilarValForms.containsKey(k)) {
                        ArrayList<String> sxs = getSimilarTerms(k);
                        valToSimilarValForms.put(k, sxs);
                        valToSimilarValFormsTokenized.put(k, StemMultiColMap.stemList(sxs));
                    }
                }

                for (List<String> exactV : exactKnownExamples.get(exactKey)) {
                    for (String v : exactV) {
                        if (!valToSimilarValForms.containsKey(v)) {
                            ArrayList<String> sys = getSimilarTerms(v);
                            valToSimilarValForms.put(v, sys);
                            valToSimilarValFormsTokenized.put(v, StemMultiColMap.stemList(sys));
                        }
                    }
                }
            }
        }


        ArrayList<String>[] xs = new ArrayList[knownExamples.keySet().iterator().next().size()];
        ArrayList<String>[] ys = new ArrayList[knownExamples.values().iterator().next().getCountsUnsorted().keySet().iterator().next().size()];

        for (int i = 0; i < xs.length; i++) {
            xs[i] = new ArrayList<String>();
        }
        for (int i = 0; i < ys.length; i++) {
            ys[i] = new ArrayList<String>();
        }

        for (List<String> x : selectedExamples.keySet()) {
            for (int i = 0; i < xs.length; i++) {
                xs[i].add(x.get(i));

                if (valToSimilarValFormsTokenized.containsKey(x.get(i))) {
                    for (String sx : valToSimilarValFormsTokenized.get(x.get(i))) {
                        xs[i].add(sx);
                    }
                }
            }
            for (List<String> y : selectedExamples.get(x).getCountsUnsorted().keySet()) {
                for (int i = 0; i < ys.length; i++) {
                    ys[i].add(y.get(i));

                    if (valToSimilarValFormsTokenized.containsKey(y.get(i))) {
                        for (String sy : valToSimilarValFormsTokenized.get(y.get(i))) {
                            ys[i].add(sy);
                        }
                    }
                }
            }
        }


        return buildAndExecuteQuery(xs, ys, examplesPerQuery);
    }

    /**
     * @param x
     * @return
     * @throws IOException
     */
    public ArrayList<String> getSimilarTerms(String x) throws IOException {
        if (x == null || x.length() < 1 / maxDistance) {
            return new ArrayList<String>();
        } else {
            ArrayList<String> results = new ArrayList<String>();

            FuzzyQuery fq = new FuzzyQuery(new Term("term", x), (int) Math.min(2, Math.ceil(maxDistance * x.length())));
            TopDocs hits = indexSearcher.search(fq, 100);
            for (ScoreDoc hit : hits.scoreDocs) {
                String term = indexReader.document(hit.doc).get("term");
                results.add(term);
            }

            return results;

        }
    }

    public static GapList<String> SubString(String str, int n) {
        GapList<String> subStrs = new GapList<>();
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j <= n; j++) {
                String subStr = str.substring(i, j);
                if (subStr.length() > 3) {
                    subStrs.add(subStr);
                }
            }
        }
        return subStrs;
    }

//    Analyzer analyzer = new Analyzer() {
//        @Override
//        protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
//            Tokenizer source = new NGramTokenizer(reader, 3, fieldName.length());
//            TokenStream filter = new LowercaseFilter(source);
//            reflectAsString(true);
//            return new TokenStreamComponents(source, filter);
//        }
//    };


    @Override
    protected void finalize() throws Throwable {
        indexReader.close();
    }


    /**
     * Tests whether all components in v1 are in range of the corresponding components in v2
     *
     * @param v1
     * @param v2
     * @return
     * @throws IOException
     */
    public boolean areSimilar(List<String> v1, List<String> v2) throws IOException {
        for (int i = 0; i < v1.size(); i++) {
            if (!getSimilarTerms(v1.get(i)).contains(v2.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ArrayList<MultiColMatchCase> findTables(Collection<String>[] xs, int examplesPerQuery) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Fuzzy matching not implemented yet");
    }
}
