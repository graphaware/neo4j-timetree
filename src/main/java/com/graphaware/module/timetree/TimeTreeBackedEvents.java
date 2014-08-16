package com.graphaware.module.timetree;

import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.domain.TimeInstant;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

import static com.graphaware.module.timetree.SingleTimeTree.*;
import static com.graphaware.module.timetree.domain.TimeTreeRelationshipTypes.*;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * {@link TimedEvents} backed by a {@link TimeTree}.
 */
@Service
public class TimeTreeBackedEvents implements TimedEvents {

    private final TimeTree timeTree;

    private static final List<String> timeTreeRelationships = getTimeTreeRelationshipNames();

    @Autowired
    public TimeTreeBackedEvents(TimeTree timeTree) {
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
    public List<Event> getEvents(TimeInstant timeInstant, RelationshipType type) {
        Node instantNode = timeTree.getInstant(timeInstant);

        if (instantNode == null) {
            return Collections.emptyList();
        }

        return getEventsAttachedToNodeAndChildren(instantNode, type);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant startTime, TimeInstant endTime, RelationshipType type) {
        List<Event> events = new ArrayList<>();

        for (TimeInstant instant : TimeInstant.getInstants(startTime, endTime)) {
            events.addAll(getEvents(instant, type));
        }

        return events;
    }

    private List<Event> getEventsAttachedToNodeAndChildren(Node parent, RelationshipType type) {
        List<Event> result = new ArrayList<>();

        Relationship firstRelationship = parent.getSingleRelationship(FIRST, OUTGOING);
        if (firstRelationship == null) {
            return getEventsAttachedToNode(parent, type);
        }

        Node existingChild = firstRelationship.getEndNode();
        while (true) {
            Relationship nextRelationship = existingChild.getSingleRelationship(NEXT, OUTGOING);

            if (nextRelationship == null || parent(nextRelationship.getEndNode()).getId() != parent.getId()) {
                break;
            }

            result.addAll(getEventsAttachedToNodeAndChildren(nextRelationship.getEndNode(), type));
            result.addAll(getEventsAttachedToNode(nextRelationship.getEndNode(), type));
            existingChild = nextRelationship.getEndNode();
        }

        return result;
    }

    private List<Event> getEventsAttachedToNode(Node node, RelationshipType type) {
        List<Event> result = new LinkedList<>();

        for (Relationship rel : node.getRelationships(INCOMING)) {
            if (!timeTreeRelationships.contains(rel.getType().name())) {
                if (type == null || (type.name().equals(rel.getType().name()))) {
                    result.add(new Event(rel.getOtherNode(node), rel.getType()));
                }
            }
        }

        return result;
    }
}
