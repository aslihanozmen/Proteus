package util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

import test.GeneralTests;
import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import com.vertica.jdbc.VerticaConnection;

public class GroundTruthCleaner {

    private static Properties verticaProperties = null;
    private static String databaseHost = "jdbc:vertica://localhost/";
    //private static String databaseHost = "jdbc:vertica://192.168.56.102/";

    public static void main(String args[]) {
        try {
            verticaProperties.load(GroundTruthCleaner.class.getResourceAsStream("/vertica.properties"));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Properties file not found");
        }
        removeSmallTableOccurencesMultiCol(args);
    }

    /**
     * @param args
     * @Deprecated use multicol version instead
     */
    public static void removeAbsent(String args[]) {
        try {
            CSVReader csvReader = new CSVReader(new InputStreamReader(
                    new FileInputStream(args[0]), "UTF-8"), ',', '"');
            String columns[] = null;
            CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(
                    new FileOutputStream(args[3]), "UTF-8"), ',', '"');
            StandardAnalyzer a = new StandardAnalyzer();
            Class.forName("com.vertica.jdbc.Driver");
            VerticaConnection con = (VerticaConnection) java.sql.DriverManager
                    .getConnection(databaseHost + verticaProperties.getProperty("database"), verticaProperties);

            PreparedStatement ps = con
                    .prepareStatement(" SELECT t1.tableid FROM "
                            + " (SELECT tableid, rowid FROM tokenized_to_col  where tokenized = "
                            + "?) AS t1,"
                            + " (SELECT tableid, rowid FROM tokenized_to_col where tokenized = "
                            + "?) AS t2" + " WHERE t1.tableid = t2.tableid"
                            + " AND t1.rowid = t2.rowid" + " LIMIT 1");

            int col1 = Integer.parseInt(args[1]);
            int col2 = Integer.parseInt(args[2]);
            while ((columns = csvReader.readNext()) != null) {
                String k = StemMap.tokenize(a, columns[col1]);
                String v = StemMap.tokenize(a, columns[col2]);

                if (k == null || k.equals("") || v == null || v.equals("")) {
                    continue;
                }
                ps.setString(1, k);
                ps.setString(2, v);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    continue;
                }

                csvWriter.writeNext(columns);
                csvWriter.flush();
            }
            ps.close();
            csvReader.close();
            csvWriter.close();

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    /**
     * @param args
     * @Deprecated use multicol version instead
     */
    public static void removeSmallTableOccurences(String args[]) {
        try {
            CSVReader csvReader = new CSVReader(new InputStreamReader(
                    new FileInputStream(args[0]), "UTF-8"), ',', '"');
            String columns[] = null;
            CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(
                    new FileOutputStream(args[3]), "UTF-8"), ',', '"');
            StandardAnalyzer a = new StandardAnalyzer();
            Class.forName("com.vertica.jdbc.Driver");
            VerticaConnection con = (VerticaConnection) java.sql.DriverManager
                    .getConnection(databaseHost + verticaProperties.getProperty("database"), verticaProperties);

            PreparedStatement ps = con
                    .prepareStatement(" SELECT t1.tableid FROM "
                            + " (SELECT tableid, rowid FROM tokenized_to_col  where tokenized = "
                            + "?) AS t1,"
                            + " (SELECT tableid, rowid FROM tokenized_to_col where tokenized = "
                            + "?) AS t2, new_tables_tokenized_full AS t3"
                            + " WHERE t1.tableid = t2.tableid"
                            + " AND t1.rowid = t2.rowid"
                            + " AND t1.tableid = t3.id" + " AND t3.numRows > 3"
                            + " LIMIT 1");

            int keyCount = 0;
            int col1 = Integer.parseInt(args[1]);
            int col2 = Integer.parseInt(args[2]);
            while ((columns = csvReader.readNext()) != null) {
                String k = StemMap.tokenize(a, columns[col1]);
                String v = StemMap.tokenize(a, columns[col2]);

                if (k == null || k.equals("") || v == null || v.equals("")) {
                    continue;
                }
                ++keyCount;
                if (keyCount % 10 == 0) {
                    System.out.println(k + " " + v);
                }
                ps.setString(1, k);
                ps.setString(2, v);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    continue;
                }

                csvWriter.writeNext(columns);
            }
            ps.close();
            csvReader.close();
            csvWriter.close();

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static void removeAbsentMultiCol(String args[]) {
        try {
            CSVReader csvReader = new CSVReader(new InputStreamReader(
                    new FileInputStream(args[0]), "UTF-8"), ',', '"');
            String columns[] = null;
            CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(
                    new FileOutputStream(args[3]), "UTF-8"), ',', '"');
            StandardAnalyzer a = new StandardAnalyzer();

            String[] colsFromStr = args[1].split(",");
            String[] colsToStr = args[2].split(",");

            int[] colsFrom = new int[colsFromStr.length];
            for (int i = 0; i < colsFrom.length; i++) {
                colsFrom[i] = Integer.parseInt(colsFromStr[i]);
            }
            int[] colsTo = new int[colsToStr.length];
            for (int i = 0; i < colsTo.length; i++) {
                colsTo[i] = Integer.parseInt(colsToStr[i]);
            }

            Class.forName("com.vertica.jdbc.Driver");
            VerticaConnection con = (VerticaConnection) java.sql.DriverManager
                    .getConnection(databaseHost + verticaProperties.getProperty("database"), verticaProperties);

            StringBuilder sql = new StringBuilder(" SELECT t1.tableid FROM ");

            for (int i = 0; i < colsFrom.length; i++) {
                sql.append(" (SELECT tableid, rowid FROM tokenized_to_col  where tokenized = "
                        + "?) AS t" + (i + 1) + ",");
            }
            for (int i = 0; i < colsTo.length; i++) {
                sql.append(" (SELECT tableid, rowid FROM tokenized_to_col  where tokenized = "
                        + "?) AS t" + (colsFrom.length + i + 1) + ",");
            }
            sql.replace(sql.length() - 1, sql.length(), " "); // remove last
            // comma
            sql.append(" WHERE 1=1");

            for (int i = 1; i < colsFrom.length; i++) {
                sql.append(" AND t1.tableid = t" + (i + 1)
                        + ".tableid AND t1.rowid = t" + (i + 1) + ".rowid");
            }
            for (int i = 0; i < colsTo.length; i++) {
                sql.append(" AND t1.tableid = t" + (colsFrom.length + i + 1)
                        + ".tableid AND t1.rowid = t" + (i + 1) + ".rowid");
            }
            sql.append(" LIMIT 1");

            PreparedStatement ps = con.prepareStatement(sql.toString());

            while ((columns = csvReader.readNext()) != null) {
                String[] k = new String[colsFrom.length];
                String[] v = new String[colsTo.length];
                for (int i = 0; i < colsFrom.length; i++) {
                    k[i] = StemMap.tokenize(a, columns[colsFrom[i]]);
                    if (k[i] == null || k[i].equals("")) {
                        continue;
                    }
                }
                for (int i = 0; i < colsTo.length; i++) {
                    v[i] = StemMap.tokenize(a, columns[colsTo[i]]);
                    if (v[i] == null || v[i].equals("")) {
                        continue;
                    }
                }
                for (int i = 0; i < colsFrom.length; i++) {
                    ps.setString(i + 1, k[i]);
                }
                for (int i = 0; i < colsTo.length; i++) {
                    ps.setString(colsFrom.length + i + 1, v[i]);
                }

                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    continue;
                }

                csvWriter.writeNext(columns);
            }
            ps.close();
            csvReader.close();
            csvWriter.close();

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static void removeSmallTableOccurencesMultiCol(String args[]) {
        try {
            CSVReader csvReader = new CSVReader(new InputStreamReader(
                    new FileInputStream(args[0]), "UTF-8"), ',', '"');
            String columns[] = null;
            CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(
                    new FileOutputStream(args[3]), "UTF-8"), ',', '"');
            StandardAnalyzer a = new StandardAnalyzer();

            String[] colsFromStr = args[1].split(",");
            String[] colsToStr = args[2].split(",");

            int[] colsFrom = new int[colsFromStr.length];
            for (int i = 0; i < colsFrom.length; i++) {
                colsFrom[i] = Integer.parseInt(colsFromStr[i]);
            }
            int[] colsTo = new int[colsToStr.length];
            for (int i = 0; i < colsTo.length; i++) {
                colsTo[i] = Integer.parseInt(colsToStr[i]);
            }

            Class.forName("com.vertica.jdbc.Driver");
            VerticaConnection con = (VerticaConnection) java.sql.DriverManager
                    .getConnection(databaseHost + verticaProperties.getProperty("database"), verticaProperties);

            StringBuilder sql = new StringBuilder(
                    " SELECT t1.tableid FROM new_tables_tokenized_full AS t");
            for (int i = 0; i < colsFrom.length; i++) {
                sql.append(", (SELECT tableid, rowid FROM tokenized_to_col  where tokenized = "
                        + "?) AS t" + (i + 1));
            }
            for (int i = 0; i < colsTo.length; i++) {
                sql.append(", (SELECT tableid, rowid FROM tokenized_to_col  where tokenized = "
                        + "?) AS t" + (colsFrom.length + i + 1));
            }
            sql.append(" WHERE t.numRows > 3 AND t.id = t1.tableid");

            for (int i = 1; i < colsFrom.length; i++) {
                sql.append(" AND t1.tableid = t" + (i + 1)
                        + ".tableid AND t1.rowid = t" + (i + 1) + ".rowid");
            }
            for (int i = 0; i < colsTo.length; i++) {
                sql.append(" AND t1.tableid = t" + (colsFrom.length + i + 1)
                        + ".tableid AND t1.rowid = t" + (i + 1) + ".rowid");
            }
            sql.append(" LIMIT 1");

            PreparedStatement ps = con.prepareStatement(sql.toString());


            int keyCount = 0;
            while ((columns = csvReader.readNext()) != null) {
                keyCount++;
                if (keyCount % 100 == 0) {
                    System.out.println(keyCount + " keys checked");
                }
                String[] k = new String[colsFrom.length];
                String[] v = new String[colsTo.length];
                for (int i = 0; i < colsFrom.length; i++) {
                    k[i] = StemMap.tokenize(a, columns[colsFrom[i]]);
                    if (k[i] == null || k[i].equals("")) {
                        continue;
                    }
                }
                for (int i = 0; i < colsTo.length; i++) {
                    v[i] = StemMap.tokenize(a, columns[colsTo[i]]);
                    if (v[i] == null || v[i].equals("")) {
                        continue;
                    }
                }
                for (int i = 0; i < colsFrom.length; i++) {
                    ps.setString(i + 1, k[i]);
                }
                for (int i = 0; i < colsTo.length; i++) {
                    ps.setString(colsFrom.length + i + 1, v[i]);
                }

                ResultSet rs = ps.executeQuery();

                if (!rs.next()) {
                    continue;
                }

                csvWriter.writeNext(columns);
                csvWriter.flush();
            }
            ps.close();
            csvReader.close();
            csvWriter.close();

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
