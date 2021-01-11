package util.index;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class IndexFromDB {

    public static void main(String[] args) {
        String dbName = args[0];
        //Connect to database
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your PostgreSQL JDBC Driver? "
                    + "Include in your library path!");
            e.printStackTrace();
            return;
        }

        System.out.println("PostgreSQL JDBC Driver Registered!");
        Connection connection = null;

        try {
            //FIXME SQL: update PostgreSQL host and credentials
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/" + dbName, "postgres", "123456");

        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }

        if (connection == null) {
            System.out.println("Failed to make connection!");
        }
        indexDocs(connection, args[1]);
    }

    /**
     * @param conn
     * @param indexDir
     */
    private static void indexDocs(Connection conn, String indexDir) {
        try {
            Analyzer a = new StandardAnalyzer();
            IndexWriterConfig iwcfg = new IndexWriterConfig(a);
            iwcfg.setOpenMode(OpenMode.CREATE_OR_APPEND);
            IndexWriter iw = new IndexWriter(FSDirectory.open(new File(indexDir).toPath()), iwcfg);

            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();

            stmt.setFetchSize(1000);
            ResultSet rs = stmt.executeQuery("SELECT * FROM files;");


            int count = 0;

            while (rs.next()) {
                Document doc = new Document();

                int id = rs.getInt("id");
                String tableContent = rs.getString("contents");

                Field pathField = new IntPoint("id", id);
                doc.add(pathField);

                doc.add(new TextField("contents", tableContent, Field.Store.NO));
                iw.addDocument(doc);

                count++;
                if (count % 1000 == 0) {
                    System.out.println("Finished " + count + " files..");
                }
            }

            rs.close();
            stmt.close();
            conn.close();
            iw.close();

//			searchDocs(indexDir);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    private static void searchDocs(String indexDir) {
        try {
            IndexReader ir = DirectoryReader.open(FSDirectory.open(new File(indexDir).toPath()));
            IndexSearcher is = new IndexSearcher(ir);


            Fields fields = MultiFields.getFields(ir);
            Terms terms = fields.terms("contents");

            TermsEnum termsEnum = terms.iterator();

            BytesRef x;
            while ((x = termsEnum.next()) != null) {
                System.out.println(x.utf8ToString());
            }

            QueryParser qp = new QueryParser("contents", new StandardAnalyzer());
            Query q = qp.parse("\"el salvador\"");

            TopDocs results = is.search(q, 100);

            ScoreDoc[] hits = results.scoreDocs;

            for (ScoreDoc hit : hits) {
                Document d = is.doc(hit.doc);

                System.out.println(d + " -->" + hit.score);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

}
