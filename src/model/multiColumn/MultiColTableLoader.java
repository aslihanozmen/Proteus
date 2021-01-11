package model.multiColumn;

        import java.io.FileNotFoundException;
        import java.io.IOException;
        import java.sql.SQLException;
        import java.util.ArrayList;
        import java.util.HashMap;

        import model.Table;
        import util.MultiColMatchCase;

public interface MultiColTableLoader {

    Table loadTable(MultiColMatchCase triple) throws SQLException, FileNotFoundException, IOException;

    HashMap<String, Double> loadIdf(ArrayList<String> terms) throws SQLException;
}
