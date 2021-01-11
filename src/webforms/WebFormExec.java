package webforms;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import webforms.index.IndexBuilder;
import webforms.index.IndexQuery;
import webforms.webquery.WebQuery;
import webforms.wrappers.FormAnalyzer;
import webforms.wrappers.HttpFormWrapper;

import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;

public class WebFormExec {

    public static void main(String args[]) {

        String inputColumn = "air port code";
        String outputColumn = "city";
        String[] xValues = {"BER", "ORD", "FRA"};
        String[] yValues = {"berlin", "chicago", "frankfurt"};
        String[] xExamples = new String[yValues.length];
        for (int i = 0; i < yValues.length; i++) {
            xExamples[i] = xValues[i];
        }

        HttpFormWrapper wrapper;
        wrapper = retrieveWrapper(inputColumn, outputColumn, xExamples, yValues);
        try {
            if (wrapper == null) {
                String[] candidateURIs = retrieveUnwrappedURLs(inputColumn, outputColumn);
                for (String uri : candidateURIs) {
                    wrapper = wrapWebform(uri, inputColumn, outputColumn, xExamples, yValues);
                    if (wrapper != null) {
                        break;
                    }
                }
                if (wrapper != null) {

                    transformValues(wrapper, xValues);
                    writeWebFormToIndex(wrapper.getUrl(), inputColumn,
                            outputColumn, xValues, yValues, wrapper);

                }
            } else {
                transformValues(wrapper, xValues);

            }
        } catch (FailingHttpStatusCodeException | IOException e) {
            e.printStackTrace();
        }

    }

    public static HttpFormWrapper retrieveWrapper(String inputColumn,
                                                  String outputColumn, String[] xValues, String[] yValues) {
        IndexQuery iQ = new IndexQuery();
        return iQ.queryIndex(inputColumn);
    }

    public static String[] retrieveUnwrappedURLs(String inputColumn,
                                                 String outputColumn) {
        WebQuery gq = new WebQuery();
//		String googleSearchString = gq.makeSearchString(inputColumn + " to "
//				+ outputColumn, 1, 8);
//		List<String> candidateURIs = gq.getURLs(googleSearchString);
        String query = inputColumn + " to " + outputColumn;
        List<String> candidateURIs = gq.getBingResults(query);

        return candidateURIs.toArray(new String[candidateURIs.size()]);
    }

    public static HttpFormWrapper wrapWebform(String candidateURI,
                                              String inputColumn, String outputColumn, String[] xExamples,
                                              String[] yExamples) {
        FormAnalyzer fa = new FormAnalyzer(inputColumn, outputColumn,
                xExamples, yExamples);
        HttpFormWrapper wrapper = null;
        try {
            HttpFormWrapper newWrapper = null;
            if (fa.analyze(candidateURI)) {
                newWrapper = fa.getGetWrapper();

            }
            URL url = new URL(candidateURI);
            String base = url.getProtocol() + "://" + url.getHost();
            if (!base.equals(candidateURI) && newWrapper == null) {
                if (fa.analyze(base)) {
                    newWrapper = fa.getGetWrapper();
                }
            }
            if (wrapper == null && newWrapper != null) {
                wrapper = newWrapper;
            } else if (newWrapper != null) {
                if (wrapper.getTransformedExamples() < newWrapper
                        .getTransformedExamples()) {
                    wrapper = newWrapper;
                }
            }
            if (wrapper != null) {
                if (wrapper.getTransformedExamples() == yExamples.length) {
                    return wrapper;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return wrapper;
    }

    public static void writeWebFormToIndex(String url, String Xcolumn,
                                           String Ycolumn, String[] xValuesArray, String[] yValuesArray,
                                           HttpFormWrapper wrapper) {
        try {
            IndexBuilder iBuilder = new IndexBuilder();

            iBuilder.indexItem(url, "", Xcolumn, Ycolumn,
                    createString(xValuesArray), createString(yValuesArray),
                    wrapper);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String createString(String[] array) {
        StringBuilder sb = new StringBuilder();
        for (String arrayValue : array) {
            sb.append(arrayValue).append(" ");
        }
        return sb.toString();
    }

    public static String[] transformValues(HttpFormWrapper wrapper, String[] xs)
            throws FailingHttpStatusCodeException, MalformedURLException,
            IOException {
        WebFormExecuter executor = new WebFormExecuter(wrapper);
        return executor.transformValues(xs);
    }

    public static FormAnalyzer getFormAnalyzer(String inputColumn, String outputColumn, String[] xExamples, String[] yExamples) {
        return new FormAnalyzer(inputColumn, outputColumn, xExamples, yExamples);
    }

}
