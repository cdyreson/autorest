/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package autowebservices.joingraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Paths table represents the paths (usually shortest) that connect pairs of
 * nodes
 *
 * @author Curtis Dyreson & Arihant Jain
 */
class PathsTable {

    private Map<String, Map<String, List<Path>>> table;

    PathsTable() {
        table = new HashMap<>();
    }

    Boolean contains(String fromTable, String toTable) {
        if (!table.containsKey(fromTable)) {
            return false;
        }
        Map<String, List<Path>> map = table.get(fromTable);
        return map.containsKey(toTable);
    }

    List<Path> getPaths(String fromTable, String toTable) {
        if (!table.containsKey(fromTable)) {
            return null;
        }
        Map<String, List<Path>> map = table.get(fromTable);
        if (!map.containsKey(toTable)) {
            return null;
        }
        return map.get(toTable);
    }

    void addPath(Path path) {
        addPath(path.getStart(), path.getEnd(), path);
    }

    void addPath(String fromTable, String toTable, Path path) {
        if (!table.containsKey(fromTable)) {
            table.put(fromTable, new HashMap<>());
        }
        Map<String, List<Path>> map = table.get(fromTable);
        if (!map.containsKey(toTable)) {
            map.put(toTable, new ArrayList<>());
        }
        List<Path> paths = map.get(toTable);
        paths.add(path);
    }
}
