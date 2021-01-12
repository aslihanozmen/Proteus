package dwtc;

import com.opencsv.*;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import org.apache.tika.io.IOUtils;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class IdfVerticaWriter {

    private int NUMBER_OF_TABLES = 145542475;
    private final int BATCHSIZE = 20000;

    public void writeToDatabase(Connection con, String filePath) throws SQLException, IOException {
        initializeTable(con);
        ResultSet rs = con.createStatement().executeQuery("SELECT count(*) FROM new_tables_tokenized_full");
        while (rs.next()) {
            NUMBER_OF_TABLES = rs.getInt(1);
        }

        HashMap<String, Integer> occurenceMap = readMapFromCSV(filePath);
        int processed = 0;

        VerticaCopyStream idfStream = new VerticaCopyStream(
                (VerticaConnection) con, "COPY inverse_document_frequency"
                + " (term , number_occurence, idf) "
                + " FROM STDIN "
                + " DELIMITER ',' ENCLOSED BY '\"' DIRECT"
                + " REJECTED DATA '/home/olib92/logs/rejectedTables.txt'");
        idfStream.start();

        for (String key : occurenceMap.keySet()) {
            StringWriter stringWriter = new StringWriter();
            CSVWriter idfWriter = new CSVWriter(stringWriter, ',', '"', '\\');
            idfWriter.writeNext(new String[]{key,
                    Integer.toString(occurenceMap.get(key)),
                    Double.toString(Math.log(NUMBER_OF_TABLES / occurenceMap.get(key)))});
            idfWriter.flush();
            idfWriter.close();
            String idfString = stringWriter.toString();
            idfStream.addStream(IOUtils.toInputStream(idfString));

            processed++;
            if ((processed % BATCHSIZE) == 0) {
                idfStream.execute();
                if (idfStream.getRejects().size() != 0)
                    System.out.println(idfStream.getRejects().size() + "idfs rejected");
            }
        }
        idfStream.execute();
        if (idfStream.getRejects().size() != 0)
            System.out.println(idfStream.getRejects().size() + "idfs rejected");
        idfStream.finish();
        System.err.printf("Finished writing idf scores! %d rows processed.", processed);
    }

    private HashMap<String, Integer> readMapFromCSV(String filePath) throws IOException {
        HashMap<String, Integer> map = new HashMap<>();

        Reader reader = Files.newBufferedReader(Paths.get(filePath));
        CSVParser parser = new CSVParserBuilder().withSeparator('|').withQuoteChar('"').build();
        CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).withSkipLines(1).build();

        String[] nextRecord;
        while ((nextRecord = csvReader.readNext()) != null) {
            int count = 0;
            try {
                count = Integer.valueOf(nextRecord[1]);
            } catch (NumberFormatException e) {
            }
            map.put(nextRecord[0], count);
        }
        return map;
    }

    private void initializeTable(Connection con) throws SQLException {
        con.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS inverse_document_frequency (" +
                "term VARCHAR(256) PRIMARY KEY NOT NULL," +
                "number_occurence INT," +
                "idf FLOAT" +
                ")" +
                "SEGMENTED BY HASH(term) ALL NODES");
    }

    public static void main(String[] args) throws SQLException, IOException {
        Connection con = java.sql.DriverManager.getConnection("url", "user", "password");
        String file = "/home/olib92/dwtc/full.csv";
        try {
            file = args[0];
        } catch (Exception e){
            System.err.println("Using default configuration!");
        }

        IdfVerticaWriter verticaWriter = new IdfVerticaWriter();
        verticaWriter.writeToDatabase(con, file);
    }
}
