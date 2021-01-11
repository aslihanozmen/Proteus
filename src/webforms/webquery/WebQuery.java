package webforms.webquery;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.util.ajax.JSON;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;

import net.billylieurance.azuresearch.AzureSearchResultSet;
import net.billylieurance.azuresearch.AzureSearchWebQuery;
import net.billylieurance.azuresearch.AzureSearchWebResult;

public class WebQuery {
    final static String apiKey = "AIzaSyAohxg7ccKRrYt4W3wXdNgPVojPUHcg98M";
    final static String customSearchEngineKey = "*";

    private static Pattern patternDomainName;
    private Matcher matcher;
    private static final String DOMAIN_NAME_PATTERN = "https?://([a-zA-Z0-9]([a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])?\\.)+[a-zA-Z]{2,6}.*\\.(html+)";

    static {
        patternDomainName = Pattern.compile(DOMAIN_NAME_PATTERN);
    }

    public static void main(String args[]) {
        WebQuery gq = new WebQuery();
        gq.getDataFromBingCognitive(" Ziawasch");

    }

    // base url for the search main.java.query
    final static String searchURL = "http://ajax.googleapis.com/ajax/services/search/web?v=1.0&rsz=5";

    public String getDomainName(String url) {

        String domainName = "";
        matcher = patternDomainName.matcher(url);
        if (matcher.find()) {
            domainName = matcher.group(0).toLowerCase().trim();
        }
        return domainName;

    }

    public List<String> getBingResults(String query) {
        List<String> urls = new ArrayList<String>();
        AzureSearchWebQuery aq = new AzureSearchWebQuery();
        aq.setAppid("LUT3Sip7hWGfk1LBSYTjmahhdP0XOBmZbqoaToq8ZLc");
        aq.setQuery(query);
        // The results are paged. You can get 50 results per page max.
        // This example gets 150 results
        aq.setPage(1);
        aq.doQuery();
        AzureSearchResultSet<AzureSearchWebResult> ars = aq.getQueryResult();
        for (AzureSearchWebResult result : ars) {
            System.out.println(result.getTitle());
            System.out.println(result.getUrl());
            urls.add(result.getUrl());
        }
        return urls;
    }

    public List<String> getDataFromBingCognitive(String query) {

        Set<String> result = new HashSet<String>();

        try {
            HttpClient httpclient = HttpClients.createDefault();

            URIBuilder builder = new URIBuilder("https://api.cognitive.microsoft.com/bing/v5.0/search");

            builder.setParameter("q", query.replace(" ", "+"));
            builder.setParameter("count", "10");
            builder.setParameter("offset", "0");
            builder.setParameter("mkt", "en-us");
            builder.setParameter("safesearch", "Moderate");

            URI uri = builder.build();

            System.out.println("Sending request..." + uri.toString());
            HttpGet request = new HttpGet(uri);
            request.setHeader("Ocp-Apim-Subscription-Key", "8ee46efc7c704317b7a593b810e3e83f");

            HttpResponse response = httpclient.execute(request);
            HttpEntity entity = response.getEntity();
            HashMap<String, ?> resultPage = (HashMap<String, ?>) JSON.parse(EntityUtils.toString(entity));
            HashMap<String, ?> webPages = (HashMap<String, ?>) resultPage.get("webPages");
            Object[] Links = (Object[]) webPages.get("value");

            for (Object value : Links) {
                HashMap<String, ?> link = (HashMap<String, ?>) value;
                System.out.println(link);
                System.out.println(link.get("displayUrl"));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        return new ArrayList<String>(result);
    }

    public List<String> getDataFromGoogle(String query) {

        Set<String> result = new HashSet<String>();
        String request = "https://api.datamarket.azure.com/Data.ashx/Bing/Search/Web?Query="
                + query + "&top=10";// https://www.google.com/search
        request = request.replace(" ", "+");
        System.out.println("Sending request..." + request);

        try {

            // need http protocol, set this as a Google bot agent :)
            Document doc = Jsoup
                    .connect(request)
                    .userAgent(
                            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)")
                    .timeout(5000).get();

            // get all links
            System.out.println(doc.html());
            Elements links = doc.select("a[href]");
            for (Element link : links) {

                String temp = link.attr("href");
                if (temp.startsWith("/url?q=")) {
                    // use regex to get domain name
                    result.add(getDomainName(temp));
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return new ArrayList<String>(result);
    }

    public void read(String pUrl) {
        // pUrl is the URL we created in previous step
        try {
            String charset = "UTF-8";
            URL url;
            Reader reader;
            url = new URL(pUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            reader = new InputStreamReader(url.openStream(), charset);
            GoogleResults results = new Gson().fromJson(reader,
                    GoogleResults.class);

            if (results.getResponseData() != null) {
                for (int i = 0; i < results.getResponseData().getResults()
                        .size(); i++) {
                    // System.out.println("Title: " +
                    // results.getResponseData().getResults().get(i).getTitle());
                    System.out.println(URLDecoder.decode(results
                                    .getResponseData().getResults().get(i).getUrl(),
                            "UTF-8"));
                    // System.out.println("Content: " +
                    // results.getResponseData().getResults().get(i).getContent());
                    // System.out.println();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void finance(String[] args) throws IOException {
        for (int i = 1; i < 100; i++) {
            String address = "https://www.google.com/finance/converter?a=" + i
                    + "&from=AED&to=USD";
            // System.out.println(address);
            URL url = new URL(address);
            URLConnection urlConnection = url.openConnection();
            InputStream inpurtStream = urlConnection.getInputStream();
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(inpurtStream));
            String conversionResult = "";
            while (bufferedReader.ready()) {
                conversionResult = bufferedReader.readLine();
                if (conversionResult.contains("result")) {
                    break;
                }
            }
            bufferedReader.close();
            inpurtStream.close();

            System.out.println(i
                    + " "
                    + conversionResult.substring(conversionResult
                    .indexOf("AED")));
        }
    }
}

class GoogleResults {

    private ResponseData responseData;

    public ResponseData getResponseData() {
        return responseData;
    }

    public void setResponseData(ResponseData responseData) {
        this.responseData = responseData;
    }

    static class ResponseData {
        private List<Result> results;

        public List<Result> getResults() {
            return results;
        }

        public void setResults(List<Result> results) {
            this.results = results;
        }
    }

    static class Result {
        private String url;
        private String titleNoFormatting;
        private String content;

        public String getUrl() {
            return url;
        }

        public String getTitle() {
            return titleNoFormatting;
        }

        public String getContent() {
            return content;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public void setTitle(String title) {
            this.titleNoFormatting = title;
        }

        public void setContent(String content) {
            this.content = content;
        }
    }
}