package util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * @author John Morcos
 */
public class StemMultiColMap<T> extends HashMap<List<String>, T> {
    private static StandardAnalyzer analyzer = new StandardAnalyzer();

    @Override
    public boolean containsKey(Object key) {
        List<String> keyAsList = (List<String>) key;

        return super.containsKey(stemList(keyAsList));
    }

    @Override
    public T get(Object key) {
        return super.get(stemList((List<String>) key));
    }


    @Override
    public T put(List<String> key, T value) {
        return super.put(stemList((List<String>) key), value);
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


    public static List<String> stemList(List<String> list) {
        List<String> stemmedArr = new ArrayList<String>(list.size());
        for (String s : list) {
            stemmedArr.add(StemMap.getStem(s));
        }

        return stemmedArr;
    }


    public static String[] stemList(String[] list) {
        String[] stemmedArr = new String[list.length];
        for (int i = 0; i < list.length; i++) {
            stemmedArr[i] = StemMap.getStem(list[i]);
        }

        return stemmedArr;
    }
}
