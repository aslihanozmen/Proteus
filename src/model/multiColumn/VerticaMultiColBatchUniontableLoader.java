package model.multiColumn;

import com.google.gson.Gson;
import model.BatchTableLoader;
import model.Table;
import util.MultiColMatchCase;
import util.StemMultiColHistogram;
import util.StemMultiColMap;
import util.StringUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VerticaMultiColBatchUniontableLoader implements MultiColTableLoader, BatchTableLoader {
    private HashMap<String, Table> buffer = new HashMap<>();
    private static Gson gson = new Gson();
    private static HashMap<String, Double> changedPriors = new HashMap<>();
    private Connection connection;


    public VerticaMultiColBatchUniontableLoader(Connection connection) {
        this.connection = connection;
    }


    @Override
    public HashMap<String, Double> loadIdf(ArrayList<String> terms) throws SQLException {
        HashMap<String, Double> termToIdf = new HashMap<>();
        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT term, idf FROM inverse_document_frequency WHERE term IN ('"
                        + StringUtil.join(terms, "','") + "')");
        while (rs.next()) {
            termToIdf.put(rs.getString("term"), rs.getDouble("idf"));
        }
        return termToIdf;
    }

    @Override
    public void loadTablesWithExamples(String[] tableIds, String exampleCountDir) throws SQLException {
        return;
    }

    @Override
    public void loadTables(String[] tableIds) throws SQLException {
        if (tableIds.length == 0) {
            return;
        }

        long start_query = System.currentTimeMillis();
        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT unionid, content, source, confidence, openrank, context_tokenized" +
                        " FROM new_tables_tokenized_full INNER JOIN union_tables_new" +
                        " ON new_tables_tokenized_full.id=union_tables_new.tableid" +
                        " WHERE unionid IN (" + StringUtil.join(tableIds, ",") + ")" +
                        " ORDER BY unionid");
        System.out.println("Query for Tables was " + (System.currentTimeMillis() - start_query) + " ms");

        Table currentTable;
        while (rs.next()) {
            String unionid = rs.getString("unionid");
            currentTable  = gson.fromJson(rs.getString("content"), Table.class);

            Table union = buffer.get(unionid);
            if (union == null) {
                currentTable.source = rs.getInt("source");
                currentTable.confidence = rs.getDouble("confidence");
                currentTable.openrank = Math.max((double) rs.getInt("openrank")/100, 0.0);
                currentTable.termSet = rs.getString("context_tokenized").split(" ");
                buffer.put(unionid, currentTable);
            } else {
                union.concatTables(currentTable);
                //buffer.put(unionid, union);
            }
        }//END WHILE

        return;
    }

    @Override
    public Table loadTable(MultiColMatchCase triple) {
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
}