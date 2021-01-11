package joinGraph;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class JoinabilityGraphBuilder {

    public static void mapReduce(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "candGen");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BooleanWritable.class);

        job.setMapperClass(GraphBuilderMapper.class);
        job.setReducerClass(GraphBuilderReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }


    public static void buildGraph(String indexDir) throws IOException {
        IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)));
        long x = 0;
        BufferedWriter bw = new BufferedWriter(new FileWriter("candGen.txt"));

        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        Fields fields = MultiFields.getFields(indexReader);
        Terms terms = fields.terms("contents");
        TermsEnum iterator = terms.iterator();
        BytesRef byteRef = null;


        System.out.println("Starting field: contents");
        while ((byteRef = iterator.next()) != null) {
//        	System.out.println("\n\n\n" + byteRef.utf8ToString());

            ArrayList<String> tableIDs = new ArrayList<>();

            Term term = new Term("contents", byteRef);
            TermQuery tq = new TermQuery(term);
            TopDocs hits = indexSearcher.search(tq, 1000);


            //Top-k?
            for (ScoreDoc hit : hits.scoreDocs) {
                tableIDs.add(indexReader.document(hit.doc).get("id"));
            }

            if (tableIDs.size() > 1) {
                writeIds(bw, tableIDs);
            }

            x++;
            if (x % 10000 == 0) {
                indexSearcher = null;
                indexSearcher = flushWriter(bw, indexReader, x);
            }
        }


        terms = fields.terms("context");
        iterator = terms.iterator();
        byteRef = null;

        System.out.println("Starting field: context");
        while ((byteRef = iterator.next()) != null) {
            ArrayList<String> tableIDs = new ArrayList<>();

            Term term = new Term("context", byteRef);
            TermQuery tq = new TermQuery(term);
            TopDocs hits = indexSearcher.search(tq, 1000);


            //Top-k?
            for (ScoreDoc hit : hits.scoreDocs) {
                tableIDs.add(indexReader.document(hit.doc).get("id"));
            }

            if (tableIDs.size() > 1) {
                writeIds(bw, tableIDs);
            }

            x++;
            if (x % 10000 == 0) {
                indexSearcher = null;
                indexSearcher = flushWriter(bw, indexReader, x);
            }
        }


        terms = fields.terms("header");
        iterator = terms.iterator();
        byteRef = null;

        System.out.println("Starting field: header");
        while ((byteRef = iterator.next()) != null) {
            ArrayList<String> tableIDs = new ArrayList<>();

            Term term = new Term("header", byteRef);
            TermQuery tq = new TermQuery(term);
            TopDocs hits = indexSearcher.search(tq, 1000);


            //Top-k?
            for (ScoreDoc hit : hits.scoreDocs) {
                tableIDs.add(indexReader.document(hit.doc).get("id"));
            }

            if (tableIDs.size() > 1) {
                writeIds(bw, tableIDs);
            }

            x++;
            if (x % 10000 == 0) {
                indexSearcher = null;
                indexSearcher = flushWriter(bw, indexReader, x);
            }
        }


        terms = fields.terms("title");
        iterator = terms.iterator();
        byteRef = null;

        System.out.println("Starting field: title");
        while ((byteRef = iterator.next()) != null) {
            ArrayList<String> tableIDs = new ArrayList<>();

            Term term = new Term("title", byteRef);
            TermQuery tq = new TermQuery(term);
            TopDocs hits = indexSearcher.search(tq, 1000);


            //Top-k?
            for (ScoreDoc hit : hits.scoreDocs) {
                tableIDs.add(indexReader.document(hit.doc).get("id"));
            }

            if (tableIDs.size() > 1) {
                bw.write(tableIDs.get(0));
                for (int i = 1; i < tableIDs.size(); i++) {
                    bw.write("," + tableIDs.get(i));
                }
                bw.write('\n');
            }

            x++;
            if (x % 10000 == 0) {
                indexSearcher = null;
                indexSearcher = flushWriter(bw, indexReader, x);
            }
        }


        indexReader.close();
        bw.flush();
        bw.close();
        System.out.println(x + " lines printed");
        System.out.println("Done");
    }

    private static IndexSearcher flushWriter(BufferedWriter bw, IndexReader indexReader, long x) throws IOException {
        bw.flush();
        System.out.println(x + " lines printed");
        System.gc();
        return new IndexSearcher(indexReader);
    }

    private static void writeIds(BufferedWriter bw, ArrayList<String> tableIDs) throws IOException {
        bw.write(tableIDs.get(0));
        for (int i = 1; i < tableIDs.size(); i++) {
            bw.write("," + tableIDs.get(i));
        }
        bw.write('\n');
    }
}
