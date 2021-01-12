package webforms.index;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import webforms.wrappers.HttpFormWrapper;

public class IndexQuery {

    public HttpFormWrapper queryIndex(String inputColumn) {
        Analyzer analyzer = new StandardAnalyzer();
        // the "title" arg specifies the default field to use
        // when no field is explicitly specified in the main.java.query.
        Query q;
        try {
            MultiFieldQueryParser mfqp = new MultiFieldQueryParser(new String[]{"content", "inputColumn", "outputColumn"}, analyzer);
            mfqp.setDefaultOperator(Operator.OR);
            q = mfqp.parse(inputColumn);
            if (!DirectoryReader.indexExists(FSDirectory.open(new File(Config.getProperty("indexDir")).toPath()))) {
                return null;
            }
            IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(Config.getProperty("indexDir")).toPath()));
            IndexSearcher searcher = new IndexSearcher(reader);
            //TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);

            TopDocs hits = searcher.search(q, 10);

            // 4. display results
            for (ScoreDoc doc : hits.scoreDocs) {
                Document d = searcher.doc(doc.doc);
                return wrapFromDocument(d);
            }
//TODO generate wrapper from document
            // reader can only be closed when there
            // is no need to access the documents any more.
            reader.close();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. search

        return null;
    }

    private HttpFormWrapper wrapFromDocument(Document d) {
        HttpFormWrapper wrapper = new HttpFormWrapper(d.get("requestType"), d.get("requestUrl"), d.get("inputParam"), d.get("outputPath"),
                parseSelectIDs(d.get("selectBuffer")), Integer.parseInt(d.get("outputPathIndex")), parseOutputPath(d.get("pathOptions")));
        System.out.println(wrapper.toString());
        return wrapper;
    }

    private Map<String, String> parseSelectIDs(String string) {
        String[] keyValues = string.split(" ");
        Map<String, String> selectIDs = new HashMap<String, String>();
        if (string.length() < 2) {
            return selectIDs;
        }
        for (int i = 0; i < keyValues.length; i = i + 2) {
            selectIDs.put(keyValues[i], keyValues[i + 1]);
        }
        return selectIDs;
    }

    private TObjectIntMap<String> parseOutputPath(String string) {
        String[] keyValues = string.split(" ");
        TObjectIntMap<String> outputOptions = new TObjectIntHashMap<String>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            outputOptions.put(keyValues[i], Integer.parseInt(keyValues[i + 1]));
        }
        return outputOptions;
    }

}
