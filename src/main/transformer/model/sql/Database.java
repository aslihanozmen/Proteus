package main.transformer.model.sql;

import main.transformer.lookup.bean.CandidateKey;
import main.transformer.model.LookupSource;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.magicwerk.brownies.collections.GapList;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class Database implements AutoCloseable, LookupSource {

    private Connection connection;
    static final Logger logger = Logger.getLogger(Database.class);

    public Database(String file) throws Exception {
        connection = DriverManager.getConnection("jdbc:h2:mem:");
        Statement statement = connection.createStatement();
        for (String line : FileUtils.readLines(new File(file), Charset.defaultCharset())) {
            if (line != null && line.trim().length() > 0) {
                statement.execute(line);
            }
        }

    }

    @Override
    public Set<CandidateKey> findCandidateKeys() {
        return Collections.emptySet();
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public List<String> getTables() throws Exception{
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES where table_schema = 'PUBLIC'");
        GapList<String> result = new GapList<>();
        while (resultSet.next()) {
            result.add(resultSet.getString("TABLE_NAME"));
        }
        return result;
    }

    //C.S
    @Override
    public Map<String, CandidateKey> allDataKeys(Set<CandidateKey> keys,  List<String> inputs) throws Exception{
        Statement statement = connection.createStatement();
        Map<String, CandidateKey> result = new HashMap<>();
        for (CandidateKey key : keys) {
            ResultSet rs = statement.executeQuery("SELECT "+key.getColumn()+" FROM " + key.getTable() );
            while( rs.next()){
                result.put(rs.getString(1), key);
            }
        }
        return result;
    }

    @Override
    public Set<CandidateKey> findDataKeys(Set<CandidateKey> keys, List<String> input) throws Exception {
        Statement statement = connection.createStatement();
        HashSet<CandidateKey> result = new HashSet<>();
        for (CandidateKey key : keys) {
            if (statement.executeQuery("SELECT " + key.getColumn() + " FROM " + key.getTable() + " where " + key.getColumn() + " in (" + input.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")) + ")").next()) {
                result.add(key);
            }
        }
        return result;
    }

    @Override
    public List<String> getColumnNames(String tableName) throws Exception {
        Statement statement = connection.createStatement();
        try (ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '" + tableName + "'")) {
            List<String> result = new GapList<>();
            while (resultSet.next()) {
                result.add(resultSet.getString("COLUMN_NAME"));
            }
            return result;
        }
    }

    @Override
    public List<String> queryAllSolution(Set<String> srcKey, Map<String, List<String>> param, CandidateKey destKey) throws Exception{
        Statement statement = connection.createStatement();
        List<String> results=new ArrayList<>();

        for (String src : srcKey) {
            if (param.get(src) == null) {
                return null;
            }
        }
        List<String> columnNames = getColumnNames(destKey.getTable());
        final String whereClause = srcKey.stream()
                .flatMap(column -> param.get(column).stream().map(oneParam -> "'" + oneParam + "' in (" + String.join(",", columnNames) + ")"))
                .collect(Collectors.joining(" AND "));

        logger.info("filtering :"+whereClause+", content from column:"+destKey.getColumn()+", table:"+destKey.getTable());
        ResultSet resultSet = statement.executeQuery("SELECT " + destKey.getColumn() + " FROM " + destKey.getTable() + " where " + whereClause);

        while( resultSet.next()){
            results.add(resultSet.getString(destKey.getColumn()));
        }
        if( results.isEmpty() ) {
            return Collections.singletonList(null);
        }else
            return results;
    }

    @Override
    public String query(Set<String> srcKey, Map<String, List<String>> param, CandidateKey destKey) throws Exception{
        Statement statement = connection.createStatement();
        for (String src : srcKey) {
            if (param.get(src) == null) {
                return null;
            }
        }
        List<String> columnNames = getColumnNames(destKey.getTable());
        final String whereClause = srcKey.stream()
                .flatMap(column -> param.get(column).stream().map(oneParam -> "'" + oneParam + "' in (" + String.join(",", columnNames) + ")"))
                .collect(Collectors.joining(" AND "));

        logger.info("filtering :"+whereClause+", content from column:"+destKey.getColumn()+", table:"+destKey.getTable());
        ResultSet resultSet = statement.executeQuery("SELECT " + destKey.getColumn() + " FROM " + destKey.getTable() + " where " + whereClause);

        if (resultSet.next()) {
             String response = resultSet.getString(destKey.getColumn());
             if( resultSet.next()){
                 logger.error("MORE THAN ONE RESPONSE - "+whereClause);
             }
             return response;
        }
        return null;
    }

    @Override
    public Map<String, String> findAll(String input, String table) throws Exception{
        Statement statement = connection.createStatement();
        List<String> columnNames = getColumnNames(table);
        for (String column : columnNames) {
            if (statement.executeQuery("SELECT * FROM " + table + " where " + column + " = '" + input + "'").next()) {
                HashMap<String, String> result = new HashMap<>();
                for (String otherColumn : columnNames) {
                    result.put(otherColumn, result.get(otherColumn));
                }
                return result;
            }
        }
        return Collections.emptyMap();
    }
}