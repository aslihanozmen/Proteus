package util;

import java.io.File;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.jena.query.text.EntityDefinition;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.tika.io.IOUtils;
import au.com.bytecode.opencsv.CSVWriter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.TDB;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDFS;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;


public class KBVerticaLoader {

    private static Properties verticaProperties = null;
    private static String databaseHost = "jdbc:vertica://localhost/";
    //private static String databaseHost = "jdbc:vertica://192.168.56.102/";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: <KB dir>");
            System.exit(0);
        }

        String kbDir = args[0];
        try {

            verticaProperties.load(KBVerticaLoader.class.getResourceAsStream("/vertica.properties"));

            StandardAnalyzer analyzer = new StandardAnalyzer();
            Connection con = java.sql.DriverManager.getConnection(databaseHost + verticaProperties.getProperty("database"), verticaProperties);

            con.setAutoCommit(false);


            int batchSize = 100000;
            int processed = 0;

            System.out.println("Batch size: " + batchSize);

            int id = 0;


            VerticaCopyStream tabsStream = new VerticaCopyStream(
                    (VerticaConnection) con, "COPY dbpedia_triples"
                    + " (id , subject, predicate, object, object_tokenized)"
                    + " FROM STDIN "
                    + " DELIMITER ',' ENCLOSED BY '\"' DIRECT"
                    + " REJECTED DATA '/home/olib92/tr.txt'");
            tabsStream.start();


            Dataset baseDataset = TDBFactory.createDataset(kbDir + File.separator + "data");
            TDB.sync(baseDataset);


            // Define the index mapping
            EntityDefinition entDef = new EntityDefinition("uri", "text", RDFS.label.asNode());


            StringWriter swt = new StringWriter();
            CSVWriter tabsWriter = new CSVWriter(swt, ',', '"', '\\');

            Model model = baseDataset.getDefaultModel();
            StmtIterator iter = model.listStatements();

            while (iter.hasNext()) {
                Statement stmt = iter.nextStatement();  // get next statement
                Resource subject = stmt.getSubject();     // get the subject
                Property predicate = stmt.getPredicate();   // get the predicate
                RDFNode object = stmt.getObject();      // get the object

                String objectAsString = null;
                String objectTokenized = null;

                if (object.isResource()) {
                    objectAsString = object.asResource().getURI();
                    objectTokenized = null;
                } else {
                    // object is a literal
                    objectAsString = object.asLiteral().getString();
                    objectTokenized = StemMap.tokenize(analyzer, objectAsString);
                }
                id++;
                try {

                    tabsWriter.writeNext(new String[]{
                            Integer.toString(id),
                            subject.getURI(),
                            predicate.getURI(),
                            objectAsString,
                            objectTokenized
                    });


                    processed++;


                    if (processed % batchSize == 0) {
                        tabsWriter.flush();
                        tabsWriter.close();

                        String tabsStr = swt.toString();
                        tabsStream.addStream(IOUtils.toInputStream(tabsStr));


                        tabsStream.execute();

                        System.out.println(tabsStream.getRejects().size() + " rejects");

                        swt = new StringWriter();
                        tabsWriter = new CSVWriter(swt, ',', '"', '\\');


                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
            tabsWriter.flush();
            tabsWriter.close();

            String tabsStr = swt.toString();
            tabsStream.addStream(IOUtils.toInputStream(tabsStr));

            tabsStream.execute();

            System.out.println(tabsStream.getRejects().size() + "t rejects");
            tabsStream.finish();

            con.commit();
            analyzer.close();

            model.close();
            baseDataset.close();

            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
