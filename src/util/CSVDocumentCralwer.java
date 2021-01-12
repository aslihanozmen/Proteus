package util;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

public class CSVDocumentCralwer {
    public static HashSet<File> crawlDocs(String directoryPath) throws IOException {
        File directory = new File(directoryPath);
        HashSet<File> files = new HashSet<>();
        crawl(directory, files);

        System.out.println("Crawled the directory. " + files.size() + " documents loaded");

        return files;
    }

    /**
     * Recursive method that crawls the documents
     *
     * @param f
     * @param files
     * @throws IOException
     */
    private static void crawl(File f, HashSet<File> files) throws IOException {
        if (f.isFile() && f.getName().endsWith(".csv")) {
            files.add(f);
            return;
        } else if (f.isDirectory()) {
            File[] subFiles = f.listFiles();

            for (File subF : subFiles) {
                crawl(subF, files);
            }
            return;
        }
    }
}
