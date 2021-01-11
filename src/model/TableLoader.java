package model;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

import util.MatchCase;

public interface TableLoader {
    public Table loadTable(MatchCase t) throws SQLException, FileNotFoundException, IOException;


}
