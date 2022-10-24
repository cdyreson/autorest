/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package autowebservices.database;

import java.util.List;

/**
 *
 * @author Curtis Dyreson & Arihant Jain
 */
public class ForeignKey {

    private String fromTable;
    private String toTable;
    List<String> fromColumns;
    List<String> toColumns;

    ForeignKey(String fromTable, String toTable, List<String> fromColumns, List<String> toColumns) {
        this.fromTable = fromTable;
        this.toTable = toTable;
        this.fromColumns = fromColumns;
        this.toColumns = toColumns;
    }

    void addFromColumn(String col) {
        fromColumns.add(col);
    }

    void addToColumn(String col) {
        toColumns.add(col);
    }

    public String getFromTable() {
        return fromTable;
    }

    public String getToTable() {
        return toTable;
    }

    public String generateJoinCondition() {
        StringBuilder result = new StringBuilder();
        int size = fromColumns.size();
        while (size-- > 0) {
            result.append(fromTable).append(".\"").append(fromColumns.get(size)).append("\" = ").append(toTable).append(".\"").append(toColumns.get(size)).append("\"");
            if (size > 0) {
                result.append(" AND ");
            }
        }
        return result.toString();
    }

    public String getColumnJoin() {
        return fromColumns.get(0);
    }
}
