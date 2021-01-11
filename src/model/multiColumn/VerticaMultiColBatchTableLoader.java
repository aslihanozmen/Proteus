package model.multiColumn;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import model.BatchTableLoader;
import model.Table;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

import util.*;

import com.google.gson.Gson;

public class VerticaMultiColBatchTableLoader implements MultiColTableLoader, BatchTableLoader {
    private HashMap<String, Table> buffer = new HashMap<>();
    private static Gson gson = new Gson();
    private static HashMap<String, Double> changedPriors = new HashMap<>();
    private Connection connection;


    public VerticaMultiColBatchTableLoader(Connection connection) {
        this.connection = connection;
    }

    public HashMap<String, Table> getBuffer() {
        return buffer;
    }

    public void setBuffer(HashMap<String, Table> buffer) {
        this.buffer = buffer;
    }

    public static Gson getGson() {
        return gson;
    }

    public static void setGson(Gson gson) {
        VerticaMultiColBatchTableLoader.gson = gson;
    }

    public static HashMap<String, Double> getChangedPriors() {
        return changedPriors;
    }

    public static void setChangedPriors(HashMap<String, Double> changedPriors) {
        VerticaMultiColBatchTableLoader.changedPriors = changedPriors;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

 // TODO - for substring and based on word extraction

//    @Override
//    public HashMap<String, Double> loadIdf(ArrayList<String> terms) throws SQLException {
//        HashMap<String, Double> termToIdf = new HashMap<>();
//        System.out.println(StringUtil.join(terms, "','"));
//        StringBuilder sb = new StringBuilder();
//        for(String term : terms) {
//            sb.append(" OR term LIKE '%").append(term).append("%'");
//        }
//        ResultSet rs = connection.createStatement().executeQuery(
//                "SELECT term, idf FROM inverse_document_frequency WHERE term IN ('"
//                        + StringUtil.join(terms, "','") + "')"
//                        + sb.toString()
//        );
//        while (rs.next()) {
//            termToIdf.put(rs.getString("term"), rs.getDouble("idf"));
//        }
//        sb.setLength(0);
//        return termToIdf;
//    }

    @Override
    public HashMap<String, Double> loadIdf(ArrayList<String> terms) throws SQLException {
        HashMap<String, Double> termToIdf = new HashMap<>();
        System.out.println(StringUtil.join(terms, "','"));
        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT term, idf FROM inverse_document_frequency WHERE term IN ('"
                        + StringUtil.join(terms, "','") + "')");
        while (rs.next()) {
            termToIdf.put(rs.getString("term"), rs.getDouble("idf"));
        }
        return termToIdf;
    }

    @Override
    public void loadTables(String[] tableIds) throws SQLException {
        if (tableIds.length == 0) {
            return;
        }

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT id, content, source, confidence, openrank, context_tokenized FROM new_tables_tokenized_full WHERE id IN ("
                        + StringUtil.join(tableIds, ",") + ")");

        while (rs.next()) {
            String tableid = rs.getString("id");
            Table table = gson.fromJson(rs.getString("content"), Table.class);
            table.source = rs.getInt("source");

            table.confidence = rs.getDouble("confidence");
            table.openrank = Math.max((double) rs.getInt("openrank") / 100, 0.0);
            table.termSet = rs.getString("context_tokenized").split(" ");
            buffer.put(tableid, table);
        }

        return;
    }
    public void createDirectory(String dirPath) throws IOException {
        Path path = Paths.get(dirPath);
        if(!Files.exists(Paths.get(dirPath))) {
            Files.createDirectory(path);
            System.out.println("Directory is created!");
        }
//        else {
//            System.out.println("Directory is already exist!");
//        }

    }

//    public File createFile(String filePath) {
//        return new File(filePath);
//    }
//
//    public void saveFile(Table table, String tableId, String exampleCountDir) throws IOException{
//        //For tables
//        String filePath = exampleCountDir + "/"  + tableId + ".csv" ;
//        createFile(filePath);
//        try {
//            table.saveToCSVFile(filePath);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    @Override
    public void loadTablesWithExamples(String[] tableIds, String exampleCountDir) throws SQLException, IOException {
        if (tableIds.length == 0) {
            return;
        }

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT id, content, source, confidence, openrank, context_tokenized FROM new_tables_tokenized_full WHERE id IN ("
                        + StringUtil.join(tableIds, ",") + ")");

        while (rs.next()) {
            String tableid = rs.getString("id");
            Table table = gson.fromJson(rs.getString("content"), Table.class);
            table.source = rs.getInt("source");

            // For benchmark of Semantic Transformation by Microsoft
//            try {
//                saveFile(table, tableid, exampleCountDir);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }

            table.confidence = rs.getDouble("confidence");
            table.openrank = Math.max((double) rs.getInt("openrank") / 100, 0.0);
            table.termSet = rs.getString("context_tokenized").split(" ");
            buffer.put(tableid, table);
        }
        System.out.println("**********Tables with Examples***********: " + buffer.keySet());
        System.out.println("**********Amount of Tables with Examples***********: " + buffer.keySet().size());
        return;
    }

    @Override
    public Table loadTable(MultiColMatchCase triple) throws SQLException, FileNotFoundException, IOException {
        Table table = buffer.get(triple.tableID);
        if (changedPriors.containsKey(triple.tableID)) {
            table.confidence = changedPriors.get(triple.tableID);
        }
        return table;
    }

    public static void setPrior(String tableID, double prior) {
        changedPriors.put(tableID, prior);
    }

    public static double getPrior(String tableID) {
        if (changedPriors.containsKey(tableID)) {
            return changedPriors.get(tableID);
        } else {
            return 0.5;
        }
    }


    public static void addTable(Table table, int source, Connection con) throws SQLException {
        StandardAnalyzer analyzer = new StandardAnalyzer();


        boolean autoCommit = con.getAutoCommit();
        con.setAutoCommit(false);
        con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        //Get the id
        int id = -1;
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT MAX(id) AS m FROM new_tables_tokenized_full");
        rs.next();
        id = rs.getInt(1) + 1;
        rs.close();
        stmt.close();


        PreparedStatement ps = con.prepareStatement(
                "INSERT INTO new_tables_tokenized_full "
                        + " (id , externalid, numRows, url,title, title_tokenized,"
                        + " context, context_tokenized, content, confidence, source, openrank) "
                        + " VALUES "
                        + " (?,?,?,?,?,?,?,?,?,?,?,?)");

        ps.setInt(1, id);
        ps.setString(2, null);
        ps.setInt(3, table.getNumRows());
        ps.setString(4, null); //URL of the form?
        ps.setString(5, null);
        ps.setString(6, null);
        ps.setString(7, null);
        ps.setString(8, null);
        ps.setString(9, gson.toJson(table));
        ps.setDouble(10, 0.8);
        ps.setInt(11, source);
        ps.setDouble(12, table.openrank);

        ps.execute();
        ps.close();


        String insertCellSQL = "INSERT INTO main_tokenized "
                + " (tableid, colid, rowid, term, tokenized) "
                + " VALUES (?,?,?,?,?)";

        for (int j = 0; j < table.getNumCols(); j++) {
            for (int i = 0; i < table.getNumRows(); i++) {
                ps = con.prepareStatement(insertCellSQL);
                ps.setInt(1, id);
                ps.setInt(2, j);
                ps.setInt(3, i);
                ps.setString(4, table.getCell(i, j).getValue());
                ps.setString(5,
                        StemMap.tokenize(analyzer, table.getCell(i, j).getValue()));

                ps.execute();
                ps.close();
            }
        }

        con.commit();
        con.setAutoCommit(autoCommit);

        analyzer.close();

    }

    public static void removeAddedTables(Connection con) throws SQLException {
        boolean autoCommit = con.getAutoCommit();
        con.setAutoCommit(false);
        con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        Statement stmt = con.createStatement();
        stmt = con.createStatement();
        stmt.executeUpdate("DELETE FROM main_tokenized WHERE tableid IN (SELECT id FROM new_tables_tokenized_full WHERE source > 1)");
        stmt.close();


        stmt = con.createStatement();
        stmt.executeUpdate("DELETE FROM new_tables_tokenized_full WHERE source > 1");
        stmt.close();

        con.commit();
        con.setAutoCommit(autoCommit);
    }

    public ArrayList<String> getUntokenizedVersion(List<String> vals, MultiColMatchCase table) throws SQLException {
        ArrayList<String> untokenizedVersion = new ArrayList<>();
        for (String v : vals) {
            untokenizedVersion.add(getUntokenizedVersion(v, table));
        }
        return untokenizedVersion;
    }

    public String getUntokenizedVersion(String v, MultiColMatchCase table) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(
                "SELECT term FROM main_tokenized WHERE tokenized = ?"
                        + " AND tableid = " + table.tableID);

        ps.setString(1, v);
        ResultSet rs = ps.executeQuery();
        rs.next();
        String term = rs.getString("term");
        rs.close();
        ps.close();

        return term;
    }
}
