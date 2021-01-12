package model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.gson.Gson;

import util.MatchCase;

public class VerticaTableLoader implements TableLoader {

    private Connection connection = null;
    private Gson gson = new Gson();

    public VerticaTableLoader(String dbURL, String username, String password) {
        try {
            Class.forName("com.vertica.jdbc.Driver");

            connection = DriverManager.getConnection(dbURL, username, password);

        } catch (SQLException e) {

            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        if (connection == null) {
            System.out.println("Failed to make connection!");
        }
    }


    @Override
    public synchronized Table loadTable(MatchCase triple) throws SQLException {
        String intId = triple.tableID;
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(
                "SELECT content, source, confidence, openrank, context_tokenized FROM new_tables_tokenized_full WHERE id = " + intId);

        Table table = null;

        if (rs.next()) {
            table = gson.fromJson(rs.getString("content"), Table.class);
            table.confidence = rs.getDouble("confidence");
            table.source = rs.getInt("source");
            table.openrank = Math.max((double) rs.getInt("openrank")/100, 0.0);
            table.termSet = rs.getString("context_tokenized").split(" ");
        }
        rs.close();
        stmt.close();


        return table;
    }


    @Override
    protected void finalize() throws Throwable {
        connection.close();
    }

}
