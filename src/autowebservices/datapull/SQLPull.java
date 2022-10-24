/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package autowebservices.datapull;

import autowebservices.database.ForeignKey;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.util.*;

/**
 * @author Curtis Dyreson & Arihant Jain
 */
public class SQLPull {

    public String generateQuery(Set<ForeignKey> result, String columns, List<String> tableList) {
        Set<String> tablesSet = new HashSet<>();
        if (result != null) {
            for (ForeignKey fk : result) {
                tablesSet.add(fk.getFromTable());
                tablesSet.add(fk.getToTable());
            }
        }

        tablesSet.addAll(tableList);
        String tables = tablesSet.toString();
        tables = tables.replace("[", "");
        tables = tables.replace("]", "");
        columns = columns.replace("[", "");
        columns = columns.replace("]", "");
        tables = tables.replaceAll(" ", "");
        String[] tableArr = tables.split(",");
        HashSet<String> tableCanBeUsed = new HashSet<>();
        tableCanBeUsed.add(tableArr[0]);
        ArrayList<String> listOfUniqueFkConditions = new ArrayList<>();
        HashSet<String> uniqueFkConditions = new HashSet<>();
        StringBuilder leftjoin = new StringBuilder("FROM " + tableArr[0] + " ");

        if (result != null) {
            for (ForeignKey fk : result) {
                ArrayList<String> arrayList = new ArrayList<>();
                arrayList.add(fk.generateJoinCondition().split("=")[0].replaceAll(" ", ""));
                arrayList.add(fk.generateJoinCondition().split("=")[1].replaceAll(" ", ""));
                Collections.sort(arrayList);
                if (uniqueFkConditions.add(arrayList.toString())) {
                    listOfUniqueFkConditions.add(fk.generateJoinCondition());
                }
            }
        }

        boolean flag = true;
        if (listOfUniqueFkConditions.size() > 0)
            flag = false;
        while (listOfUniqueFkConditions.size() != 0) {
            leftjoin.append("\n");
            String fkCondition = addNextConditionInQuery(tableCanBeUsed, listOfUniqueFkConditions);
            if (fkCondition == null && flag) {
                String table = listOfUniqueFkConditions.get(0).replaceAll(" ", "").split("=")[0].split("\\.")[0];
                tableCanBeUsed.add(table);
                leftjoin.append(", ").append(table).append(" ");
            } else {
                if (fkCondition != null) {
                    String str1 = fkCondition.replaceAll(" ", "").split("=")[0].split("\\.")[0];
                    String str2 = fkCondition.replaceAll(" ", "").split("=")[1].split("\\.")[0];
                    if (!tableCanBeUsed.contains(str1)) {
                        leftjoin.append("LEFT JOIN ").append(str1);
                        tableCanBeUsed.add(str1);
                    } else {
                        leftjoin.append("LEFT JOIN ").append(str2);
                        tableCanBeUsed.add(str2);
                    }
                    leftjoin.append(" ON ").append(fkCondition);
                }
            }
        }

        StringBuilder leftOutTables = new StringBuilder();
        for (String str : tableArr) {
            if (!tableCanBeUsed.contains(str)) {
                leftOutTables.append(str).append(", ");
                tableCanBeUsed.add(str);
            }
        }

        String[] orderOfTables = columns.replaceAll(" ", "").split(",");
        for (int i = 0; i < orderOfTables.length; i++) orderOfTables[i] = orderOfTables[i].split("\\.")[0];
        if (!leftOutTables.toString().equals(""))
            leftjoin = new StringBuilder("FROM " + leftOutTables + leftjoin.toString().split("FROM ")[1]);

        String s = "";
        s = "SELECT DISTINCT " + columns + "\n" + s;
        s += leftjoin;
        s += "\nORDER BY " + columns;
        return s;
    }


    public String generateRowsEstimation(Set<ForeignKey> result, String columns, List<String> tableList) {
        return "EXPLAIN " + generateQuery(result, columns, tableList);
    }

    private String addNextConditionInQuery(HashSet<String> tableCanBeUsed, ArrayList<String> listOfUniqueFkConditions) {
        for (String str : listOfUniqueFkConditions) {
            String tempStr = str;
            if (str.contains("AND"))
                str = str.split(" AND ")[0];
            int count1 = 0;
            int count2 = 0;
            String[] orderOfTables = str.replaceAll(" ", "").split("=");
            for (int i = 0; i < orderOfTables.length; i++) orderOfTables[i] = orderOfTables[i].split("\\.")[0];
            if (tableCanBeUsed.contains(orderOfTables[0])) count1++;
            if (tableCanBeUsed.contains(orderOfTables[1])) count1++;
            if (!tableCanBeUsed.contains(orderOfTables[0])) count2++;
            if (!tableCanBeUsed.contains(orderOfTables[1])) count2++;
            if ((count1 == 1 && count2 == 1) || count1 == 2) {
                if (tempStr.contains("AND"))
                    listOfUniqueFkConditions.remove(tempStr);
                else listOfUniqueFkConditions.remove(str);

                if (count1 == 2)
                    return null;
                else return str;
            }
        }
        return null;
    }

    public String changeQueryToAddSecondTable(String query, List<String> listColumns, String colJoin) {
        boolean flag = true;
        HashSet<String> hashSet = new HashSet<>();
        List<String> list = new ArrayList<>();
        for (String col : listColumns) {
            if (hashSet.add(col)) {
                list.add("t1." + col.split("\\.")[1]);
                if (colJoin == null) {
                    if (flag) {
                        flag = false;
                        colJoin = col.split("\\.")[1];
                    }
                }
            } else {
                list.add("t2." + col.split("\\.")[1]);
            }
        }
        String columns = list.toString().split("\\[")[1].split("]")[0];
        return "SELECT DISTINCT " + columns + " \nFROM (" + query + ") t1 \n" +
                "LEFT JOIN " + "(" + query + ") t2 \n" +
                "ON " + "t1." + colJoin + " = " + "t2." + colJoin + " \nORDER BY " + columns;
    }

    private String readAllBytesInAFile(String filePath) {
        String content = "";
        try {
            content = new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }

    public int getCountForValues(String filePath) {
        String str = readAllBytesInAFile(filePath);
        str = str.replaceAll("\\s+", "");
        String findStr = "\":\"";
        int lastIndex = 0;
        int count = 0;
        while (lastIndex != -1) {
            lastIndex = str.indexOf(findStr, lastIndex);
            if (lastIndex != -1) {
                count++;
                lastIndex += findStr.length();
            }
        }
        return count;
    }

    public String createAddPaths(List<ForeignKey> arrayList) {
        StringBuilder addPath = new StringBuilder();
        for (ForeignKey foreignKey : arrayList)
            addPath.append(foreignKey.getFromTable()).append(",").append(foreignKey.getToTable()).
                    append(",").append(foreignKey.getColumnJoin()).append("@");
        return addPath.toString();
    }

    public void writeToFile(String queryorderby, HashMap<String, Integer> temp, HashMap<Integer,
            Set<ForeignKey>> allPaths) throws IOException {
        FileWriter fileWriter = new FileWriter("generatedfiles/queries.txt");
        Iterator it = temp.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            fileWriter.write((pair.getKey() + "!!!" + pair.getValue()) + "!!!");
            it.remove();
        }

        if (!allPaths.isEmpty() && !queryorderby.equals("")) {
            String[] tempOrderBy = queryorderby.split("\"");
            StringBuilder orderBy = new StringBuilder("ORDER BY ");
            for (int i = 1; i < tempOrderBy.length - 1; i = i + 2)
                orderBy.append("\"").append(tempOrderBy[i]).append("\"").append(", ");
            orderBy.append("\"").append(tempOrderBy[tempOrderBy.length - 1]).append("\"");
            fileWriter.write(orderBy.toString());
        }
        fileWriter.close();
    }

    public JSONArray convertQueryResultToJson(ResultSet resultSet) {
        try {
            JSONArray jsonArray = new JSONArray();
            while (resultSet.next()) {
                int total_rows = resultSet.getMetaData().getColumnCount();
                for (int i = 0; i < total_rows; i++) {
                    JSONObject obj = new JSONObject();
                    obj.put(resultSet.getMetaData().getColumnLabel(i + 1)
                            .toLowerCase(), resultSet.getObject(i + 1));
                    jsonArray.put(obj);
                }
            }
            return jsonArray;
        } catch (Exception e) {
            return new JSONArray();
        }
    }

    public String hydrateJson(String filePath, String[] fillArray, int count) {
        StringBuilder newSchema = generateFillableSchema(filePath);
        int num = 0;
        String[] fillObj = new String[count];
        StringBuilder finalOut = new StringBuilder();
        finalOut.append("[");
        for (String s : fillArray) {
            fillObj[num++] = s;
            if (num == count) {
                finalOut.append(fillObjectInJson(newSchema.toString(), fillObj)).append(",");
                num = 0;
            }
        }
        finalOut.append("]");
        finalOut.deleteCharAt(finalOut.length() - 2);
        return finalOut.toString();
    }

    private String fillObjectInJson(String schema, String[] fillObject) {
        for (String s : fillObject)
            schema = schema.replaceFirst("\"\"", s);
        return schema;
    }

    public StringBuilder generateFillableSchema(String filePath) {
        String schema = readAllBytesInAFile(filePath);
        schema = schema.replaceAll("\\s+", "");
        String[] str = schema.split("");
        for (int i = 0; i < str.length; i++) {
            if (str[i].equals("\"") && str[i - 1].equals(":")) {
                if (str[i - 2].equals("\"")) {
                    i++;
                    while (!str[i].equals("\"")) {
                        str[i++] = "";
                    }
                }
            }
        }
        StringBuilder newSchema = new StringBuilder();
        for (String s : str) newSchema.append(s);
        return newSchema;
    }
}
