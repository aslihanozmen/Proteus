package dwtc;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.opencsv.CSVWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class CountOccurenceWorker implements Runnable {

    private Path path;
    private HashMap<String, LongAdder> map;
    private static final String[] HEADER = new String[]{"term", "count"};
    private static Gson gson;

    private static ArrayList<HashMap<String, LongAdder>> mapList;

    private static File workingDir;

    public CountOccurenceWorker(Path path, HashMap<String, LongAdder> map) {
        this.path = path;
        this.map = map;
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            System.err.println("Please provide a directory with dwtc files (gz).");
            System.exit(99);
        }

        workingDir = new File(args[0]);
        mapList = new ArrayList<>();
        gson = new Gson();
        File[] files = workingDir.listFiles((File file, String name) -> name.endsWith(".json.gz"));
        Arrays.sort(files);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        for (int i = 0; i < files.length; i++) {
            Runnable worker = new CountOccurenceWorker(files[i].toPath(), new HashMap<>());
            executor.execute(worker);
            Thread.sleep(500);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        Map<String, Integer> full = mergeMapsFromList();
        try {
            Writer writer = Files.newBufferedWriter(Paths.get(workingDir + File.separator + "full.csv"));
            CSVWriter csvwriter = new CSVWriter(writer, '|', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
            csvwriter.writeNext(HEADER);
            for (String key : full.keySet()) {
                csvwriter.writeNext(new String[]{key, String.valueOf(full.get(key))});
            }
            csvwriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Integer> mergeMapsFromList() {
        Map<String, Integer> map = new HashMap<>();

        for (HashMap<String, LongAdder> m : mapList) {
            map = Stream.concat(map.entrySet().stream(), m.entrySet().stream()).collect(Collectors.toMap(
                    entry -> entry.getKey(),
                    entry -> entry.getValue().intValue(),
                    Integer::sum
            ));
        }
        return map;
    }

    @Override
    public void run() {
        String filename = path.getFileName().toString().split("\\.")[0];
        System.err.println("Started ExportWorker " + filename);
        GZIPInputStream gzipStream = null;
        try {
            InputStream input = Files.newInputStream(path);
            gzipStream = new GZIPInputStream(new BufferedInputStream(input));
        } catch (IOException e) {
            return;
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream));
        List<String> content = reader.lines().collect(Collectors.toList());

        // count occurence in relation in addition to termset
        for (String line : content) {
            JsonObject json = gson.fromJson(line, JsonObject.class);
            JsonArray termset = json.getAsJsonArray("termSet");
            for (int i = 0; i < termset.size(); i++) {
                String term = termset.get(i).getAsString();
                map.computeIfAbsent(term, k -> new LongAdder()).increment();
            }
        }
        content = null;
        mapList.add(map);
        try {
            writeMapToCsv(filename + ".csv");
            gzipStream.close();
            gzipStream = null;
            reader.close();
            reader = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void writeMapToCsv(String filename) throws IOException {
        System.err.println("Writing map to csv - " + filename);
        Writer writer = Files.newBufferedWriter(Paths.get(workingDir + File.separator + "maps" + File.separator + filename));
        CSVWriter csvwriter = new CSVWriter(writer, '|', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);

        csvwriter.writeNext(HEADER);
        for (String key : map.keySet()) {
            csvwriter.writeNext(new String[]{key, String.valueOf(map.get(key))});
        }

        csvwriter.close();
    }
}