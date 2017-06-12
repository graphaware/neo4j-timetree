package com.graphaware.module.timetree.proc;

import org.neo4j.graphdb.Node;

/**
 * Created by alberto.delazzari on 19/05/17.
 */
public class NodeResult {

    public final Node node;

    public NodeResult(Node node) {
        this.node = node;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o != null && getClass() == o.getClass() && node.equals(o);
    }

    @Override
    public int hashCode() {
        return node.hashCode();
    }
}
