/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package autowebservices.joingraph;

import autowebservices.database.ForeignKey;

/**
 * An Edge is a FK relationship in the Graph
 *
 * @author Curtis Dyreson & Arihant Jain
 */
class  Edge {
    private ForeignKey foreignKey;

    Edge(ForeignKey fk) {
        foreignKey = fk;
    }
    
    ForeignKey getForeignKey() {
        return foreignKey;
    }

}
