/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package autowebservices.database;

import java.sql.*;
import java.util.*;

/**
 * The DB class encapsulates the database actions.
 *
 * @author Curtis Dyreson & Arihant Jain
 */
public class DB {

    private Connection conn;
    private ResultSet rs = null;
    private DatabaseMetaData metadata;
    private Map<String, Set<String>> columnLookup;
    private Set<String> tableSet;
    private Map<String, Set<String>> fkColumns;

    // Connect to the database and build all of the info for the graph
    public DB(String url, String dbName, String uName, String pwd) throws SQLException {
        conn = DriverManager.getConnection(url + dbName, uName, pwd);
        metadata = conn.getMetaData();
        tableSet = new HashSet<>();
        List<String> tables = this.getTableNames();
        tableSet.addAll(tables);
        this.buildFKs();
        // Build the column names
        columnLookup = new HashMap<>();
        for (String table : tables) {
            List<String> names = this.getColumnNames(table);
            for (String col : names) {
                if (fkColumns.containsKey(table)) {
                    if (fkColumns.get(table).contains(col)) {
                        continue;
                    }
                }
                if (!columnLookup.containsKey(col)) {
                    columnLookup.put(col, new HashSet<>());
                }
                Set<String> colTables = columnLookup.get(col);
                colTables.add(table);

            }
        }
    }

    public Set<String> stringLookup(String key) {
        Set<String> result = columnLookup(key);
        if (result.isEmpty()) {
            result.add(tableLookup(key));
        }
        return result;
    }

    private String tableLookup(String key) {
        if (tableSet.contains(key)) {
            return key;
        }
        System.err.println("Table not found in tableLookup: " + key);
        return null;
    }

    private Set<String> columnLookup(String key) {
        if (columnLookup.containsKey(key)) {
            return columnLookup.get(key);
        } else {
            System.err.println("Table not found in columnLookup: " + key);
            return new HashSet<>();
        }
    }

    /**
     * Execute a query
     *
     * @param query string
     * @return Result of the query in a ResultSet
     */
    public ResultSet executeQuery(String query) throws SQLException {
        Statement stmt = conn.createStatement();
        rs = stmt.executeQuery(query);
        return rs;
    }

    /**
     * getTables returns a list of strings
     */
    public List<String> getTableNames() throws SQLException {
        String[] table = {"TABLE"};
        List<String> tables;
        // receive the Type of the object in a String array.
        rs = metadata.getTables(null, null, null, table);
        tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"));
        }
        return tables;
    }

    public List<ForeignKey> buildFKs() throws SQLException {
        List<ForeignKey> fks = new ArrayList<>(10);
        Map<String, ForeignKey> keyMap = new HashMap<>();
        fkColumns = new HashMap<>();
        rs = metadata.getCrossReference(null, null, null, null, null, null);
        while (rs.next()) {
            String name = rs.getString("FK_NAME");
            String tabName = rs.getString("FKTABLE_NAME");
            String tabFromName = rs.getString("PKTABLE_NAME");
            name = tabName + "." + name;
            if (keyMap.containsKey(name)) {
                ForeignKey fk = keyMap.get(name);
                fk.addFromColumn(rs.getString("PKCOLUMN_NAME"));
                fk.addToColumn(rs.getString("FKCOLUMN_NAME"));
            } else {
                List<String> fc = new ArrayList<>(1);
                List<String> tc = new ArrayList<>(1);
                fc.add(rs.getString("PKCOLUMN_NAME"));
                tc.add(rs.getString("FKCOLUMN_NAME"));
                keyMap.put(name, new ForeignKey(rs.getString("PKTABLE_NAME"), rs.getString("FKTABLE_NAME"), fc, tc));
            }
            // Remove foreign key columns from column lookup by caching in a map
            String colName = rs.getString("FKCOLUMN_NAME");

            if (!tabName.equals(tabFromName)) {
                if (!fkColumns.containsKey(tabName)) {
                    fkColumns.put(tabName, new HashSet<>());
                }
                fkColumns.get(tabName).add(colName);
            }
        }

        // Reverse every foreign key
        Map<String, ForeignKey> reversedKeyMap = new HashMap<>();
        for (String name : keyMap.keySet()) {
            ForeignKey fk = keyMap.get(name);
            String ft = fk.getFromTable();
            String tt = fk.getToTable();
            List<String> fc = fk.fromColumns;
            List<String> tc = fk.toColumns;

            // Code to ensure manufactured name not used before
            String newName = name + " reversed";
            int count = 0;
            while (keyMap.containsKey(newName)) {
                newName += count++;
            }
            reversedKeyMap.put(newName, new ForeignKey(tt, ft, tc, fc));
        }

        for (String name : reversedKeyMap.keySet()) {
            keyMap.put(name, reversedKeyMap.get(name));
        }

        for (String key : keyMap.keySet()) {
            fks.add(keyMap.get(key));
        }
        return fks;
    }

    private List<String> getColumnNames(String table) throws SQLException {
        List<String> names = new ArrayList<>();
        rs = metadata.getColumns(null, null, table, null);
        while (rs.next()) {
            names.add(rs.getString("COLUMN_NAME"));
        }
        return names;
    }

    public String getPrimaryKey(String table) {
        String query = "SELECT c.column_name, c.data_type\n" +
                "FROM information_schema.table_constraints tc \n" +
                "JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) \n" +
                "JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema\n" +
                "  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name\n" +
                "WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '" + table + "';";
        try (ResultSet resultSet = executeQuery(query)) {
            resultSet.next();
            return resultSet.getString("COLUMN_NAME");
        } catch (SQLException e) {
            System.err.println("Query not executed");
        }
        return "";
    }
}
