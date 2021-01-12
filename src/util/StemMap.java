package util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class StemMap<T> extends HashMap<String, T> {
    private static StandardAnalyzer analyzer = new StandardAnalyzer();

    @Override
    public boolean containsKey(Object key) {
        return super.containsKey(getStem(((String) key)));
    }


    public static String getStem(String s) {
        if (s == null) {
            return null;
        }
        //return s.toLowerCase().replaceAll("\\s+", " ").trim();
        return StemMap.tokenize(analyzer, s);
    }


    @Override
    public T get(Object key) {
        return super.get(getStem((String) key));
    }


    @Override
    public T put(String key, T value) {
        return super.put(getStem((String) key), value);
    }


    public static String tokenize(Analyzer analyzer, String s) {
        if (s == null) {
            return null;
        }
        List<String> result = new ArrayList<String>();
        try {
            TokenStream stream = analyzer.tokenStream(null, s.toLowerCase());
            stream.reset();
            while (stream.incrementToken()) {
                result.add(stream.getAttribute(CharTermAttribute.class).toString());
            }
            stream.close();


        } catch (IOException e) {
            // not thrown b/c we're using a string reader...
            throw new RuntimeException(e);
        }
        return StringUtil.join(result, " ");
    }


}
