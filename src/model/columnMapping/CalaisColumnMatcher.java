package model.columnMapping;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import model.Table;
import mx.bigdata.jcalais.CalaisClient;
import mx.bigdata.jcalais.CalaisObject;
import mx.bigdata.jcalais.CalaisResponse;
import mx.bigdata.jcalais.rest.CalaisRestClient;
import util.CalaisEntity;

public class CalaisColumnMatcher extends AbstractColumnMatcher {
    private static final CalaisClient calaisClient = new CalaisRestClient("n7c5yyrz337hsfyabj7sd2ug");

    private HashMap<String, CalaisEntity> cache = new HashMap<String, CalaisEntity>();
    private String cacheFile = "calaisCache.txt";

    public CalaisColumnMatcher(String cacheFile) {
        this.cacheFile = cacheFile;
        try {
            loadCache();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error: could not load file " + cacheFile + ". Will use an empty cache!");
        }
    }


    /**
     * Loads cache information from the file
     */
    private void loadCache() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(cacheFile));

        String line = null;
        while ((line = br.readLine()) != null) {
            StringTokenizer tokenizer = new StringTokenizer(line, "====");
            String value = tokenizer.nextToken();

            CalaisEntity entity = null;

            if (tokenizer.hasMoreTokens()) {
                String calaisEntityStr = tokenizer.nextToken();
                if (calaisEntityStr.equals("") || calaisEntityStr.equalsIgnoreCase("null")) {
                    entity = null;
                } else {

                    entity = new CalaisEntity();


                    tokenizer = new StringTokenizer(calaisEntityStr, ",");
                    entity.uri = tokenizer.nextToken();
                    entity.typeURI = tokenizer.nextToken();
                    if (entity.typeURI.equals("") || entity.typeURI.equalsIgnoreCase("null")) {
                        entity.typeURI = null;
                    }
                    while (tokenizer.hasMoreTokens()) {
                        String resolvedURI = tokenizer.nextToken();
                        if (resolvedURI.equals("") || resolvedURI.equalsIgnoreCase("null")) {
                            break;
                        }
                    }
                }
            }

            cache.put(value, entity);
        }

        br.close();
    }


    @Override
    public int getCoverage(Table inputTable, int anchorCol, Table webTable, int webCol) {
        int matchesFound = 0;

        List<String> webRows = webTable.getCellsinCol(webCol);
        List<String> inputRows = inputTable.getCellsinCol(anchorCol);


        //Some cleaning
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


        //Now the comparisons
        for (String webRow : webRows) {
            for (String inputRow : inputRows) {
                try {
                    CalaisEntity webEntity = getEntity(webRow);
                    CalaisEntity inputEntity = getEntity(inputRow);

                    if (webEntity != null && inputEntity != null && webEntity.equals(inputEntity)) {
                        matchesFound++;
                    } else if (webEntity == null && inputEntity == null && webRow.equalsIgnoreCase(inputRow)) {
                        matchesFound++;
                    }
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }

        return matchesFound;
    }


    public CalaisEntity getEntity(String value) throws IOException {
        if (cache.containsKey(value)) {
            return cache.get(value);
        }
        CalaisEntity entity = getEntityObject(identifyEntity(value));
        cache.put(value, entity);

        saveCache();

        return entity;
    }

    public static CalaisEntity getEntityObject(CalaisObject calaisObj) {
        if (calaisObj == null) {
            return null;
        }

        CalaisEntity entity = new CalaisEntity();

        entity.uri = calaisObj.getField("_uri");
        entity.typeURI = calaisObj.getField("_typeReference");

        Iterable<Map<String, String>> resolutions = (Iterable<Map<String, String>>) calaisObj.getList("resolutions");
        if (resolutions != null) {
            for (Map<String, String> resolution : resolutions) {
                String resolvedURI = resolution.get("id");

                entity.resolvedURIs.add(resolvedURI);
            }
        }

        return entity;

    }


    /**
     * @param value
     * @return the first entity from openCalais's response, or null if none exist
     * @throws IOException
     */
    public static CalaisObject identifyEntity(String value) throws IOException {
        if (value == null) {
            return null;
        }
        CalaisResponse response = calaisClient.analyze(value.toUpperCase());

        if (!response.getEntities().iterator().hasNext()) {
            return null;
        }

        return response.getEntities().iterator().next();
    }


    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        try {
            saveCache();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void saveCache() throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(cacheFile));

        for (String s : cache.keySet()) {
            bw.write(s + "====");

            CalaisEntity e = cache.get(s);
            if (e != null) {
                bw.write(e.uri + "," + e.typeURI);

                for (String resolvedURI : e.resolvedURIs) {
                    bw.write("," + resolvedURI);
                }

            }


            bw.write("\n");
        }

        bw.close();

    }

    @Override
    public boolean computeDoMatch(String s1, String s2) {
        throw new UnsupportedOperationException();
    }
}
