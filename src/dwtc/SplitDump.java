package dwtc;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class SplitDump {

    private int batchsize = 1000000;
    private File file;
    private String outputDir;

    public SplitDump(File file, String outputDir) {
        this.file = file;
        this.outputDir = outputDir;
    }

    private void splitDomains() throws IOException {
        HashMap<String, ArrayList<String>> domainToTables = new HashMap<>();
        GZIPInputStream in = new GZIPInputStream(new FileInputStream(file));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        Path path = Paths.get(outputDir);
        //if directory exists?
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                //fail to create directory
                e.printStackTrace();
            }
        }
        int processed = 0;
        String content;
        while ((content = reader.readLine()) != null) {
            String domain = content.split("\\|")[4];
            if (domainToTables.get(domain) == null) {
                ArrayList<String> tables = new ArrayList<>();
                tables.add(content);
                domainToTables.put(domain, tables);
            } else {
                ArrayList<String> tables = domainToTables.get(domain);
                tables.add(content);
                domainToTables.put(domain, tables);
            }
            processed++;
            if ((processed % batchsize) == 0) {
                System.out.print("Writting " + NumberFormat.getInstance().format(domainToTables.size()) + " domains. ");
                writeMap(domainToTables);
                System.out.println(NumberFormat.getInstance().format(processed) + " lines done!");
                domainToTables.clear();
            }
        }
        writeMap(domainToTables);
        System.out.println(NumberFormat.getInstance().format(processed) + " lines done!");
        //domainToTables.entrySet().removeIf(entry -> entry.getValue().size() < 2);
    }

    private void writeMap(HashMap<String, ArrayList<String>> map) throws IOException {
        GZIPOutputStream out;
        BufferedWriter writer;
        for (String domain : map.keySet()) {
            out = new GZIPOutputStream(new FileOutputStream(outputDir + File.separator + domain.replaceAll("\\.", "-") + ".txt.gz", true));
            writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
            for (String line : map.get(domain)) {
                writer.write(line);
                writer.newLine();
            }
            writer.close();
            out.close();
        }
    }

    public static void main(String[] args) throws IOException {
        File file = null;
        String outputDir = "";
        try {
            file = new File(args[0]);
            outputDir = args[1];
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("Provide an input file and an output directory.");
            System.exit(12);
        }
        SplitDump spliter = new SplitDump(file, outputDir);
        spliter.splitDomains();
    }

}
