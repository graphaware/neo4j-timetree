package com.graphaware.module.timetree.proc;

import com.graphaware.module.timetree.domain.Event;
import org.neo4j.graphdb.Node;

/**
 * Created by alberto.delazzari on 19/05/17.
 */
public class EventResult {

    public final Node node;

    public final String relationshipType;

    public final String direction;

    public EventResult(Event event){
        node = event.getNode();
        relationshipType = event.getRelationshipType().name();
        direction = event.getDirection().name();
    }
}
