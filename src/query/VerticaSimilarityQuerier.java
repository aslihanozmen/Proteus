package query;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import com.vertica.jdbc.VerticaConnection;

import util.StemHistogram;
import util.StemMap;
import util.StemMultiColMap;
import util.MatchCase;

public class VerticaSimilarityQuerier extends AbstractVerticaQuerier implements TableQuerier {

    //Unstemmed version, for similarity checking
    public static float maxDistance = 0.2f;
    private IndexReader indexReader = null;
    private IndexSearcher indexSearcher = null;
    public HashMap<String, Collection<String>> exactKnownExamples;

    private Policy policy = Policy.ALL;
    private int maxExamples = -1;

    private VerticaConnection con;
    public HashMap<String, Collection<String>> keyToSimilarKeyForms;
    public HashMap<String, Collection<String>> valToSimilarValForms;
    public StemMap<Collection<String>> keyToSimilarKeyFormsTokenized;
    public StemMap<Collection<String>> valToSimilarValFormsTokenized;


    public VerticaSimilarityQuerier(VerticaConnection con, String termIndexDir) throws IOException {
        this(con, termIndexDir, Policy.ALL, -1);
    }

    public VerticaSimilarityQuerier(VerticaConnection con, String termIndexDir,
                                    Policy policy, int maxExamples) throws IOException {
        super(con, policy, maxExamples);
        indexReader = DirectoryReader.open(FSDirectory.open(new File(termIndexDir).toPath()));
        indexSearcher = new IndexSearcher(indexReader);

    }


    @Override
    public ArrayList<MatchCase> findTables(
            StemMap<StemMap<HashSet<MatchCase>>> keyToImages,
            StemMap<StemHistogram> knownExamples, int examplesPerQuery) throws IOException {


        if (examplesPerQuery > knownExamples.size()) {
            for (String k : knownExamples.keySet()) {
                for (String v : knownExamples.get(k).getCountsUnsorted().keySet()) {
                    System.out.println(k + " -> " + v);
                }
            }
            throw new IllegalStateException("|known examples| = " + knownExamples.size()
                    + ", tau = " + examplesPerQuery);
        }


        StemMap<StemHistogram> selectedExamples = null;

        try {
            selectedExamples = selectExamples(knownExamples);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (exactKnownExamples == null) {
            throw new IllegalStateException("Exact examples must be set before using the VerticaSimilarityQuerier.");
        }
        if (keyToSimilarKeyFormsTokenized == null && valToSimilarValFormsTokenized == null) {
            keyToSimilarKeyForms = new HashMap<String, Collection<String>>();
            valToSimilarValForms = new HashMap<String, Collection<String>>();

            keyToSimilarKeyFormsTokenized = new StemMap<Collection<String>>();
            valToSimilarValFormsTokenized = new StemMap<Collection<String>>();

            for (String exactKey : exactKnownExamples.keySet()) {
                ArrayList<String> sxs = getSimilarTerms(exactKey);
                keyToSimilarKeyForms.put(exactKey, sxs);
                keyToSimilarKeyFormsTokenized.put(exactKey, StemMultiColMap.stemList(sxs));

                for (String exactV : exactKnownExamples.get(exactKey)) {
                    ArrayList<String> sys = getSimilarTerms(exactV);
                    valToSimilarValForms.put(exactV, sys);
                    valToSimilarValFormsTokenized.put(exactV, StemMultiColMap.stemList(sys));
                }
            }
        }


        ArrayList<String> xs = new ArrayList<>();
        ArrayList<String> ys = new ArrayList<>();


        for (String x : selectedExamples.keySet()) {
            xs.add(x);

            if (keyToSimilarKeyFormsTokenized.containsKey(x)) {
                for (String sx : keyToSimilarKeyFormsTokenized.get(x)) {
                    xs.add(sx);
                }
            }

            for (String y : selectedExamples.get(x).getCountsUnsorted().keySet()) {
                ys.add(y);

                if (valToSimilarValFormsTokenized.containsKey(y)) {
                    for (String sy : valToSimilarValFormsTokenized.get(y)) {
                        ys.add(sy);
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


    @Override
    protected void finalize() throws Throwable {
        indexReader.close();
    }

}
