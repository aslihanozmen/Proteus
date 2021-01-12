package dwtc;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ExportWorker {


    private static void split(String dir, int tableid) {
        long start = System.currentTimeMillis();
        int filesDone = 0;
        File dirAsFile = new File(dir);
        HashMap<String, ArrayList<String>> domainTableMap = new HashMap<>();

        File[] files = dirAsFile.listFiles((File file, String name) -> name.endsWith(".json.gz"));
        Arrays.sort(files);
        Gson gson = new Gson();

        for (File file : files) {
            System.out.println("Starting file " + file.getName());
            GZIPInputStream gis = null;
            try {
                gis = new GZIPInputStream(new FileInputStream(dirAsFile.getCanonicalPath() + File.separator + file.getName()));
                BufferedReader reader = new BufferedReader(new InputStreamReader(gis));
                String content;
                String domain;
                JsonObject json;

                while ((content = reader.readLine()) != null) {
                    tableid++;
                    json = gson.fromJson(content, JsonObject.class);
                    json.addProperty("tableid", tableid);
                    domain = new URL(json.get("url").getAsString()).getHost();

                    ArrayList<String> list = domainTableMap.get(domain);
                    if (list == null) {
                        list = new ArrayList<>();
                        list.add(json.toString());
                        domainTableMap.put(domain, list);
                    } else {
                        list.add(json.toString());
                        domainTableMap.put(domain, list);
                    }
                }
                gis.close();
                reader.close();
                gis = null;
                reader = null;
                filesDone++;
                // Writing Tables to Files - one file per domain
                GZIPOutputStream gout;
                BufferedWriter writer;
                if ((filesDone % 20) == 0) {
                    for (String key : domainTableMap.keySet()) {
                        gout = new GZIPOutputStream(new FileOutputStream("output2" + File.separator + key.replaceAll("\\.", "-") + ".json.gz", true));
                        writer = new BufferedWriter(new OutputStreamWriter(gout, "UTF-8"));

                        ArrayList<String> tables = domainTableMap.get(key);
                        for (String str : tables) {
                            writer.append(str);
                            writer.newLine();
                        }
                        writer.close();
                    }// end for keys in map
                    System.out.println(filesDone + " files written in " + (System.currentTimeMillis() - start)/60000 + " min");
                    domainTableMap.clear();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        } // end for files
        try {
            for (String key : domainTableMap.keySet()) {
                GZIPOutputStream gout = new GZIPOutputStream(new FileOutputStream("output2" + File.separator + key.replaceAll("\\.", "-") + ".json.gz", true));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(gout, "UTF-8"));

                ArrayList<String> tables = domainTableMap.get(key);
                for (String str : tables) {
                    writer.append(str);
                    writer.newLine();
                }
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Done!!!");
    }

    /**
     * For Testing
     *
     * @param args
     */
    public static void main(String[] args) {
        String dir = "";
        if (args.length < 1) {
            System.out.println("Not enough arguments! Provide the file directory to be loaded!");
            System.exit(1);
        } else {
            dir = args[0];
        }
        int tableid = 0;
        try {
            tableid = Integer.valueOf(args[1]);
        } catch (Exception e) {
            System.err.println("No tableid provided! tableid = 0");
        }
        ExportWorker.split(dir, tableid);
    }
}
