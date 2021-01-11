package model.columnMapping;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import model.Table;

public class FreebaseColumnMatcher extends AbstractColumnMatcher {

    @Override
    public int getCoverage(Table inputTable, int anchorCol, Table webTable,
                           int webCol) {
        int count = 0;

        List<String> webRows = webTable.getCellsinCol(webCol);
        List<String> inputRows = inputTable.getCellsinCol(anchorCol);

        // Some cleaning
        for (int i = 0; i < webRows.size(); i++) {
            String webRow = webRows.get(i);
            if (webRow != null) {
                webRow = webRow.replaceAll("&nbsp;", " ");
                webRow = webRow.replaceAll("\\s+", " ");
                webRow = webRow.trim();

                webRows.set(i, webRow);
            }
        }
        for (int i = 0; i < inputRows.size(); i++) {
            String inputRow = inputRows.get(i);
            if (inputRow != null) {
                inputRow = inputRow.replaceAll("&nbsp;", " ");
                inputRow = inputRow.replaceAll("\\s+", " ");
                inputRow = inputRow.trim();

                inputRows.set(i, inputRow);
            }
        }

        for (String webRow : webRows) {
            try {
                // TODO: for now we are assuming distinct values
                for (String inputRow : inputRows) {

                    if (getEntitiesMatchingLabels(webRow, inputRow).size() > 0) {
                        count++;
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        return count;
    }


    public HashSet<String> getEntitiesMatchingLabel(String lbl) throws ClientProtocolException, IOException {
        HashSet<String> ids = new HashSet<>();

        String query = "[{\"name~=\" : \"" + lbl + "\", \"id\" : null}]";

        String url = "https://www.googleapis.com/freebase/v1/mqlread?&key=AIzaSyBQsJ_PIYg9yANa5zniBH4QjlRRoqsTj_0&main.java.query=" + URLEncoder.encode(query, "UTF-8");

        HttpClient client = new DefaultHttpClient();
        HttpGet get = new HttpGet(url);
        HttpResponse response = client.execute(get);


        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(EntityUtils.toString(response.getEntity()));
        JsonArray entities = element.getAsJsonObject().get("result").getAsJsonArray();
        for (int i = 0; i < entities.size(); i++) {
            JsonElement entity = entities.get(i);
            String id = entity.getAsJsonObject().get("id").getAsString();

            ids.add(id);
        }

        return ids;
    }


    public HashSet<String> getEntitiesMatchingLabels(String lbl1, String lbl2) throws ClientProtocolException, IOException {
        HashSet<String> ids = new HashSet<>();

        String query = "[{\"/common/topic/alias\" : \"" + lbl1 + "\","
                + "\"/common/topic/alias\" : \"" + lbl2 + "\","
                + "\"id\" : null}]";

        String url = "https://www.googleapis.com/freebase/v1/mqlread?&key=AIzaSyBQsJ_PIYg9yANa5zniBH4QjlRRoqsTj_0&main.java.query=" + URLEncoder.encode(query, "UTF-8");

        HttpClient client = new DefaultHttpClient();
        HttpGet get = new HttpGet(url);
        HttpResponse response = client.execute(get);


        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(EntityUtils.toString(response.getEntity()));
        JsonArray entities = element.getAsJsonObject().get("result").getAsJsonArray();
        for (int i = 0; i < entities.size(); i++) {
            JsonElement entity = entities.get(i);
            String id = entity.getAsJsonObject().get("id").getAsString();

            ids.add(id);
        }

        return ids;
    }


    public HashSet<String> getEntitiesMatchingLabelsFuzzily(String lbl1, String lbl2) throws ClientProtocolException, IOException {
        HashSet<String> ids = new HashSet<>();

        String query = "[{\"/common/topic/alias~=\" : \"" + lbl1 + "\","
                + "\"/common/topic/alias~=\" : \"" + lbl2 + "\","
                + "\"id\" : null}]";

        String url = "https://www.googleapis.com/freebase/v1/mqlread?&key=AIzaSyBQsJ_PIYg9yANa5zniBH4QjlRRoqsTj_0&main.java.query=" + URLEncoder.encode(query, "UTF-8");

        HttpClient client = new DefaultHttpClient();
        HttpGet get = new HttpGet(url);
        HttpResponse response = client.execute(get);


        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(EntityUtils.toString(response.getEntity()));
        JsonArray entities = element.getAsJsonObject().get("result").getAsJsonArray();
        for (int i = 0; i < entities.size(); i++) {
            JsonElement entity = entities.get(i);
            String id = entity.getAsJsonObject().get("id").getAsString();

            ids.add(id);
        }

        return ids;
    }

    public HashSet<String> getEntitiesMatchingLabelsWithSearching(String lbl1, String lbl2) throws ClientProtocolException, IOException {
        HashSet<String> ids = new HashSet<>();

        String query = "\"" + lbl1 + "\" \"" + lbl2 + "\"";

        String url = "https://www.googleapis.com/freebase/v1/search?&key=AIzaSyBQsJ_PIYg9yANa5zniBH4QjlRRoqsTj_0&main.java.query=" + URLEncoder.encode(query, "UTF-8");

        HttpClient client = new DefaultHttpClient();
        HttpGet get = new HttpGet(url);
        HttpResponse response = client.execute(get);


        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(EntityUtils.toString(response.getEntity()));
        JsonArray entities = element.getAsJsonObject().get("result").getAsJsonArray();
        for (int i = 0; i < entities.size(); i++) {
            JsonElement entity = entities.get(i);
            String id = entity.getAsJsonObject().get("id").getAsString();

            ids.add(id);
        }

        return ids;
    }

    @Override
    public boolean computeDoMatch(String s1, String s2) {
        throw new UnsupportedOperationException();
    }
}
