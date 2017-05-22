package com.graphaware.module.timetree.proc;

import org.neo4j.graphdb.Node;

/**
 * Created by alberto.delazzari on 19/05/17.
 */
public class Instant {

    public final Node instant;

    public Instant(Node instant) {
        this.instant = instant;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o != null && getClass() == o.getClass() && instant.equals(((Instant) o).instant);
    }

    @Override
    public int hashCode() {
        return instant.hashCode();
    }
}
