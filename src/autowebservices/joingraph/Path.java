/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package autowebservices.joingraph;

import autowebservices.database.ForeignKey;

import java.util.ArrayList;
import java.util.List;

/**
 * A Path is a list of foreign keys
 *
 * @author Curtis Dyreson & Arihant Jain
 */
public class Path {

    private List<ForeignKey> path;

    public Path() {
        path = new ArrayList<>(1);
    }

    public Path(Path clone) {
        this();
        path.addAll(clone.path);
    }

    String getStart() {
        ForeignKey fk = path.get(0);
        return fk.getFromTable();
    }

    String getEnd() {
        ForeignKey fk = path.get(path.size() - 1);
        return fk.getToTable();
    }

    public List<ForeignKey> getFKs() {
        return path;
    }

    void push(ForeignKey fk) {
        path.add(fk);
    }

}