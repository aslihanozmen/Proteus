package dwtc;

import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.net.InternetDomainName;
import com.google.gson.Gson;
import model.Table;
import model.TableSchema;
import model.Tuple;
import util.StemMap;
import util.StringUtil;
import webreduce.data.Dataset;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.tika.io.IOUtils;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Main Class that connects with a Vertica database and inserts table data from a provided directory (gzip-files).
 */

public class TableLoader {

    private Map<String, Map<TableSchema, Integer>> domainSchemaMap;
    //private static Map<String, Integer> domainOpenranks;

    public static void main(String[] args) throws Exception {
        String directory = "";

        if (args.length != 1) {
            System.out.println("Not enough arguments! Provide the file directory to be loaded!");
            System.exit(1);
        } else {
            directory = args[0];
        }
        String user = "user";
        String password = "password";
        String host = "host";
        int port=22;
        JSch jsch = new JSch();
        Session session = jsch.getSession(user, host, port);
        int lport = 5433;
        String rhost = "localhost";
        int rport = 5433;
        session.setPassword(password);
        session.setConfig("StrictHostKeyChecking", "no");
        System.out.println("Establishing Connection...");
        session.connect();
        int assinged_port = session.setPortForwardingL(lport, rhost, rport);
        System.out.println("localhost:"+assinged_port+" -> "+rhost+":"+rport);
        Properties verticaProperties = new Properties();
        verticaProperties.setProperty("user", "user");
        verticaProperties.setProperty("password", "password");
        try {
            Class.forName("com.vertica.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection con = java.sql.DriverManager.getConnection("jdbc:vertica://localhost:5433/xformer", verticaProperties);

        con.setAutoCommit(false);
        TableLoader.initializeDatabase(con);
        con.commit();

        TableLoader.tokenizeAndLoadAllIntoVertica(directory, con);
        con.commit();
        con.close();
    }

    private static void initializeDatabase(Connection con) throws SQLException {
        con.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS new_tables_tokenized_full (" +
                "id INT PRIMARY KEY NOT NULL," +
                "externalid VARCHAR(50)," +
                "numRows INT NOT NULL," +
                "url VARCHAR(400)," +
                "domain VARCHAR(100)," +
                "title VARCHAR(4000)," +
                "title_tokenized VARCHAR(4000)," +
                "pageTitle VARCHAR(4000)," +
                "pageTitle_tokenized VARCHAR(4000)," +
                "context VARCHAR(3000)," +
                "context_tokenized VARCHAR(3000)," +
                "content LONG VARCHAR(32000000)," +
                "confidence REAL ENCODING RLE," +
                "source INT ENCODING RLE," +
                "openrank INT ENCODING RLE" +
                ")" +
                "SEGMENTED BY HASH(id) ALL NODES");


        con.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS columns_tokenized (" +
                "tableid INT NOT NULL," +
                "colid INT NOT NULL," +
                "header VARCHAR(1000) ENCODING AUTO," +
                "header_tokenized VARCHAR(1000) ENCODING AUTO," +
                "PRIMARY KEY(tableid, colid)" +
                ")" +
                "SEGMENTED BY HASH(tableid) ALL NODES");

        con.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS main_tokenized (" +
                "tableid INT NOT NULL ENCODING RLE," +
                "colid INT NOT NULL," +
                "rowid INT NOT NULL," +
                "term VARCHAR(200) ENCODING AUTO," +
                "tokenized VARCHAR(200) ENCODING AUTO," +
                "PRIMARY KEY(tableid, colid, rowid)" +
                ")" +
                "SEGMENTED BY HASH(tableid) ALL NODES;");

        /**
         * Create Projections aka Materialized Views
         */
        con.createStatement().executeUpdate("CREATE PROJECTION IF NOT EXISTS header_tokenized_to_col (" +
                "header_tokenized" +
                ",header" +
                ",tableid" +
                ",colid) " +
                "AS SELECT header_tokenized, header, tableid, colid " +
                "FROM columns_tokenized " +
                "ORDER BY header_tokenized, tableid, colid " +
                "SEGMENTED BY HASH(header_tokenized) ALL NODES ");

        con.createStatement().executeUpdate("CREATE PROJECTION IF NOT EXISTS header_tokenized_to_col_b1(" +
                "header_tokenized" +
                ",header" +
                ",tableid" +
                ",colid) " +
                "AS SELECT header_tokenized, header, tableid, colid " +
                "FROM columns_tokenized " +
                "ORDER BY header_tokenized, tableid, colid " +
                "SEGMENTED BY HASH(header_tokenized) ALL NODES OFFSET 1");

        con.createStatement().executeUpdate("CREATE PROJECTION IF NOT EXISTS tokenized_to_col (" +
                "tokenized" +
                ",term" +
                ",tableid" +
                ",colid" +
                ",rowid) " +
                "AS SELECT tokenized, term, tableid, colid, rowid " +
                "FROM main_tokenized " +
                "ORDER BY tokenized, tableid, colid, rowid " +
                "SEGMENTED BY HASH(tokenized) ALL NODES");

        con.createStatement().executeUpdate("CREATE PROJECTION IF NOT EXISTS tokenized_to_col_b1 (" +
                "tokenized" +
                ",term" +
                ",tableid" +
                ",colid" +
                ",rowid) " +
                "AS SELECT tokenized, term, tableid, colid, rowid " +
                "FROM main_tokenized " +
                "ORDER BY tokenized, tableid, colid, rowid " +
                "SEGMENTED BY HASH(tokenized) ALL NODES OFFSET 1");

        con.createStatement().executeUpdate("CREATE PROJECTION IF NOT EXISTS term (" +
                "term)" +
                "AS SELECT term " +
                "FROM main_tokenized " +
                "ORDER BY term " +
                "SEGMENTED BY HASH(term) ALL NODES");

        con.createStatement().executeUpdate("CREATE PROJECTION IF NOT EXISTS term_b1 (" +
                "term) " +
                "AS SELECT term " +
                "FROM main_tokenized " +
                "ORDER BY term " +
                "SEGMENTED BY HASH(term) ALL NODES OFFSET 1");
    }

    private static void tokenizeAndLoadAllIntoVertica(String dir, Connection con) throws SQLException, IOException {
        StandardAnalyzer analyzer = new StandardAnalyzer();


        File dirAsFile = new File(dir);

        long startTime = System.currentTimeMillis();
        int filesDone = 0;
        int batchSize = 10000;
        int processed = 0;

        System.out.println("Batch size: " + batchSize);

        /**
         * Get the max tableid from the new_tables_tokenized_full table to guarantee unique table ids!
         * If no tables are stored yet, start with an table id of 0 (because the table id will be incremented by 1 to keep it unique).
         */
        int tabid = 0;
        ResultSet resultSet = con.createStatement().executeQuery("SELECT MAX(id) FROM new_tables_tokenized_full");
        if (resultSet.next()) {
            tabid = resultSet.getString(1) == null ? 0 : resultSet.getInt(1);
        }

        /**
         * Create Vertica CopyStream for every table.
         */
        VerticaCopyStream tabsStream = new VerticaCopyStream(
                (VerticaConnection) con, "COPY new_tables_tokenized_full"
                + " (id , externalid, numRows, url, domain, title, title_tokenized,"
                + " pageTitle, pageTitle_tokenized,"
                + " context, context_tokenized, content, confidence, source, openrank) "
                + " FROM STDIN "
                + " DELIMITER ',' ENCLOSED BY '\"' DIRECT"
                + " REJECTED DATA '/home/olib92/logs/rejectedTables.txt'");
        tabsStream.start();

        VerticaCopyStream colsStream = new VerticaCopyStream(
                (VerticaConnection) con, "COPY columns_tokenized"
                + " (tableid , colid, header, header_tokenized)"
                + " FROM STDIN "
                + " DELIMITER ',' ENCLOSED BY '\"' DIRECT"
                + " REJECTED DATA '/home/olib92/logs/rejectedColumns.txt'");
        colsStream.start();

        VerticaCopyStream cellsStream = new VerticaCopyStream(
                (VerticaConnection) con, "COPY main_tokenized"
                + " (tableid, colid, rowid, term, tokenized)"
                + " FROM STDIN "
                + " DELIMITER ',' ENCLOSED BY '\"' DIRECT"
                + " REJECTED DATA '/home/olib92/logs/rejectedCells.txt'");
        cellsStream.start();

        /**
         * Get all gzip-files from the provided directory and sort them alphabetically. Sorting not necessary but for
         * more convenience. Iterate over all gziped files!
         */
        File[] files = dirAsFile.listFiles((File file, String name) -> name.endsWith(".json.gz"));
        Arrays.sort(files);
        Gson gson = new Gson();

        for (File file : files) {
            String fileName = file.getName();
            System.out.println("Starting file " + fileName);

            GZIPInputStream gis = new GZIPInputStream(new FileInputStream(dirAsFile.getCanonicalPath() + File.separator + fileName));
            BufferedReader reader = new BufferedReader(new InputStreamReader(gis));
            String content;
            String domain;

            while ((content = reader.readLine()) != null) {

                Dataset dataset = Dataset.fromJson(content);
                Table table = extractTable(dataset);

                if (table.getNumCols() < 2) {
                    continue;
                }

                domain = dataset.domain;
                if (domain == null) {
                    try {
                        domain = new URL(dataset.url).getHost();
                    } catch (MalformedURLException e) {
                        domain = "";
                    }
                }

                int openrank = 0;
                try {
                    if (InternetDomainName.isValid(domain)) {
                        String topDomain = InternetDomainName.from(domain).topDomainUnderRegistrySuffix().toString();
//                        if (domainOpenranks.get(topDomain) != null) {
//                            openrank = domainOpenranks.get(topDomain);
//                            if (domain.startsWith("www.")) {
//                                domain = domain.replaceFirst("www.", "");
//                            }
////                            if (!topDomain.equals(domain)) {
////                                openrank /= 2;
////                            }
//                        }
                    }
                } catch (IllegalStateException e) {
//                    if (domainOpenranks.get(domain) != null) {
//                        openrank = domainOpenranks.get(domain);
//                    }
                }

                boolean hasHeader = false;
                for (int i = 0; i < table.getNumCols(); i++) {
                    if (table.getColumnMapping().getColumnNames()[i] == null) {
                        hasHeader = true;
                        break;
                    }
                    if (!table.getColumnMapping().getColumnNames()[i].equalsIgnoreCase("COLUMN" + i)) {
                        hasHeader = true;
                        break;
                    }
                }

                tabid++;
                // Create and add for every table in the database (tables, columns, rows aka main_tokenized)
                try {
                    /**
                     * Create input for full_table_tokenized table containing the whole input table and meta data.
                     * Only if no table from this domain with the same schema exists.
                     */
                    StringWriter stringWriter = new StringWriter();

                    CSVWriter tabsWriter = new CSVWriter(stringWriter, ',', '"', '\\');
                    tabsWriter.writeNext(Integer.toString(tabid),
                            fileName + "#" + tabid,
                            Integer.toString(table.getNumRows()),
                            dataset.url,
                            domain,
                            dataset.title,
                            StemMap.tokenize(analyzer, dataset.title),
                            dataset.pageTitle,
                            StemMap.tokenize(analyzer, dataset.pageTitle),
                            StringUtil.join(Arrays.asList(dataset.termSet), " "),
                            StemMap.tokenize(analyzer, StringUtil.join(Arrays.asList(dataset.termSet), " ")),
                            gson.toJson(table),
                            Double.toString(0.5),
                            Integer.toString(1),
                            String.valueOf(openrank));
                    tabsWriter.flush();
                    tabsWriter.close();
                    String tabsString = stringWriter.toString();
                    tabsStream.addStream(IOUtils.toInputStream(tabsString));

                    /**
                     * Create input for columns_tokenized table containing the column labels.
                     */
                    if (hasHeader) {
                        stringWriter = new StringWriter();
                        CSVWriter colsWriter = new CSVWriter(stringWriter, ',', '"', '\\');
                        for (int j = 0; j < table.getNumCols(); j++) {
                            colsWriter.writeNext(Integer.toString(tabid),
                                    Integer.toString(j),
                                    table.getColumnMapping().getColumnNames()[j],
                                    StemMap.tokenize(analyzer, table.getColumnMapping().getColumnNames()[j]));
                        }
                        colsWriter.flush();
                        colsWriter.close();
                        String colsString = stringWriter.toString();
                        colsStream.addStream(IOUtils.toInputStream(colsString));
                    }

                    /**
                     * Create input for main_tokenized containing the actual table data.
                     */
                    stringWriter = new StringWriter();
                    CSVWriter cellsWriter = new CSVWriter(stringWriter, ',', '"', '\\');
                    for (int j = 0; j < table.getNumCols(); j++) {
                        for (int k = 0; k < table.getNumRows(); k++) {
                            String value = table.getCell(k, j).getValue();
                            if (value != null && value.length() > 100) {
                                value = null;
                            }

                            cellsWriter.writeNext(Integer.toString(tabid),
                                    Integer.toString(j),
                                    Integer.toString(k),
                                    value,
                                    StemMap.tokenize(analyzer, value));
                        }
                    }
                    cellsWriter.flush();
                    cellsWriter.close();
                    String cellsString = stringWriter.toString();
                    cellsStream.addStream(IOUtils.toInputStream(cellsString));

                    /**
                     * Execute Streams when batchsize is reached
                     */
                    processed++;
                    if (processed % batchSize == 0) {
                        tabsStream.execute();
                        colsStream.execute();
                        cellsStream.execute();
                        if (tabsStream.getRejects().size() != 0)
                            System.out.println(tabsStream.getRejects().size() + "tabs rejected");
                        if (colsStream.getRejects().size() != 0)
                            System.out.println(colsStream.getRejects().size() + "cols rejected");
                        if (cellsStream.getRejects().size() != 0)
                            System.out.println(cellsStream.getRejects().size() + "values rejected");
                        System.out.println(processed + " tables written!");
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } //end while content readline()

            gis.close();
            filesDone++;
            System.out.println(dirAsFile.getCanonicalPath() + File.separator + file.getName());
            System.out.println("Finished loading tables in " + filesDone + " files in "
                    + ((System.currentTimeMillis() - startTime) * 1.0 / 60000) + " minutes");
        }// end for files

        tabsStream.execute();
        colsStream.execute();
        cellsStream.execute();
        if (tabsStream.getRejects().size() != 0)
            System.out.println(tabsStream.getRejects().size() + "tabs rejected");
        if (colsStream.getRejects().size() != 0)
            System.out.println(colsStream.getRejects().size() + "cols rejected");
        if (cellsStream.getRejects().size() != 0)
            System.out.println(cellsStream.getRejects().size() + "values rejected");

        tabsStream.finish();
        colsStream.finish();
        cellsStream.finish();
        analyzer.close();
    }

    private static String[][] sort2dArray(String[][] data) {
        Queue<Integer> indices;
        Map<String, Queue<Integer>> columnIndexMap = new HashMap<>();
        for (int i = 0; i < data[0].length; i++) {
            indices = new LinkedList<>();
            if (columnIndexMap.get(data[0][i]) == null) {
                indices.add(i);
                columnIndexMap.put(data[0][i], indices);
            } else {
                indices = columnIndexMap.get(data[0][i]);
                indices.add(i);
                columnIndexMap.put(data[0][i], indices);
            }
        }
        Arrays.sort(data[0], (s1, s2) -> {
            if ((s1 == null && s2 == null) || ("".equals(s1) && "".equals(s2))) {
                return 0;
            }
            if ("".equals(s1)) {
                return 1;
            }
            if ("".equals(s2)) {
                return -1;
            }
            return s1.compareTo(s2);
        });
        List<String> order = Arrays.asList(data[0]);

        //Create sorted relation
        String[][] orderedRelation = new String[data.length][data[0].length];
        for (int i = 0; i < data[0].length; i++) {
            orderedRelation[0][i] = order.get(i);
        }
        for (int j = 1; j < data.length; j++) {
            Map<String, Queue<Integer>> columnIndexMapCopy = new HashMap<>();
            for (String str : columnIndexMap.keySet()) {
                Queue<Integer> index = new LinkedList<>(columnIndexMap.get(str));
                columnIndexMapCopy.put(str, index);
            }
            for (int k = 0; k < data[j].length; k++) {
                orderedRelation[j][k] = data[j][columnIndexMapCopy.get(order.get(k)).poll()];
            }
        }
        return orderedRelation;
    }

    protected static Table extractTable(Dataset dataset) {
        //String[][] colBasedRelation = dataset.getRelation();
        String[][] colBasedRelation = sort2dArray(dataset.getRelation());

        Table table = new Table();
        String[] attrs = new String[colBasedRelation.length];

        for (int i = 0; i < attrs.length; i++) {
            attrs[i] = "COLUMN" + i;
        }

        TableSchema schema = new TableSchema(attrs);
        table.setSchema(schema);

        for (int i = 0; i < colBasedRelation[0].length; i++) {
            String[] row = new String[colBasedRelation.length];
            for (int j = 0; j < colBasedRelation.length; j++) {
                row[j] = colBasedRelation[j][i]; //Transposed
            }
            table.addTuple(new Tuple(row, schema, i - 1));
        }

        int result = TableFilter.hasHeader(table);
        if (result == 2) {
            Table transposedTable = new Table();
            for (Tuple tuple : table.getTuples()) {
                transposedTable.addColumn(tuple.getCell(0).getValue());
            }

            for (int i = 0; i < table.getNumCols(); i++) {
                String[] values = new String[table.getNumRows()];
                for (int j = 0; j < table.getNumRows(); j++) {
                    values[j] = table.getRow(j).getCell(i).getValue();
                }
                Tuple t = new Tuple(values, transposedTable.getColumnMapping(), i - 1);
                transposedTable.addTuple(t);
            } //end for each Column

            table = transposedTable;
        } else if (result == 1) {
            //New names
            for (int i = 0; i < attrs.length; i++) {
                schema.getColumnNames()[i] = colBasedRelation[i][0];
            }
            //remove the first tuple because ... ?
            table.removeTupleAtIndex(0);
        } else if (result == 0 && dataset.hasHeader) {
            //New names
            for (int i = 0; i < attrs.length; i++) {
                schema.getColumnNames()[i] = colBasedRelation[i][0];
            }
            //remove the first tuple because ... ?
            table.removeTupleAtIndex(0);
        }
        return table;
    }
}