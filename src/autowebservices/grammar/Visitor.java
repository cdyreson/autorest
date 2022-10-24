/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package autowebservices.grammar;

import autowebservices.database.DB;
import autowebservices.joingraph.Graph;
import autowebservices.tree.PatternTree;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Curtis Dyreson & Arihant Jain
 */
class Visitor {

    private static PatternTree tree;
    private static DB db;
    private static Stack<PatternTree> treeStack;
    private static Graph joinGraph;

    static void enterJson(DB db) {
        treeStack = new Stack<>();
        Visitor.db = db;
        tree = new PatternTree(db);
        joinGraph = new Graph(db);
    }

    static void exitJson() throws IOException, SQLException {
        tree.computeTreePaths(joinGraph);
    }

    static void enterArray() {
        PatternTree child = new PatternTree(db);
        treeStack.push(tree);
        tree = child;
    }

    static void exitArray() {
        tree = treeStack.pop();
    }

    static void enterObj() {
        treeStack.push(tree);
        PatternTree child = new PatternTree(db);
        treeStack.push(tree);
        tree = child;
    }

    static void exitObj() {
        tree = treeStack.pop();
    }

    static void enterPair(String key) {
        key = key.replace("\"", "");
        tree = treeStack.peek();
        PatternTree child = new PatternTree(db, tree, key);
        tree.addChild(child);
        tree = child;
        treeStack.push(child);
    }

    static void parsedString(String value) {
        value = value.replace("\"", "");
        tree = treeStack.pop();
        tree.buildPotentialLabels(value);
    }

    static void exitPair() {
        tree = treeStack.peek();
    }
}
