package model;

import java.io.IOException;
import java.sql.SQLException;

public interface BatchTableLoader {

    void loadTables(String[] tableIds) throws SQLException;

    void loadTablesWithExamples(String[] tableIds, String exampleCountDir) throws SQLException, IOException;
}