package com.graphaware.module.timetree;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class TimeTreeTimedEvents implements TimedEvents {

    private final TimeTree timeTree;

    public TimeTreeTimedEvents(TimeTree timeTree) {
        this.timeTree = timeTree;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void attachEvent(Node event, RelationshipType relationshipType, TimeInstant timeInstant) {
        event.createRelationshipTo(timeTree.getOrCreateInstant(timeInstant), relationshipType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant timeInstant) {
        return getEvents(timeInstant, (RelationshipType) null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant startTime, TimeInstant endTime) {
        return getEvents(startTime, endTime, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant timeInstant, RelationshipType relationshipType) {
        List<Event> events = new ArrayList<>();
        List<String> timeTreeRelationships = TimeTreeRelationshipTypes.getTimeTreeRelationshipNames();
        Node timeInstantNode = timeTree.getOrCreateInstant(timeInstant);
        for (Relationship rel : timeInstantNode.getRelationships()) {
            if (!timeTreeRelationships.contains(rel.getType().name())) {
                if (relationshipType == null || (rel.isType(relationshipType))) {
                    events.add(new Event(rel.getOtherNode(timeInstantNode), timeInstant, rel.getType()));
                }
            }
        }

        return events;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant startTime, TimeInstant endTime, RelationshipType eventRelation) {
        List<Event> events = new ArrayList<>();
        List<String> timeTreeRelationships = TimeTreeRelationshipTypes.getTimeTreeRelationshipNames();

        for (TimeInstant instant : TimeInstant.getInstants(startTime, endTime)) {
            Node instantNode = timeTree.getOrCreateInstant(instant);
            for (Relationship rel : instantNode.getRelationships()) {
                if (!timeTreeRelationships.contains(rel.getType().name())) {
                    if (eventRelation == null || (eventRelation.name().equals(rel.getType().name()))) {
                        events.add(new Event(rel.getOtherNode(instantNode), instant, rel.getType()));
                    }
                }
            }
        }

        return events;
    }
}
