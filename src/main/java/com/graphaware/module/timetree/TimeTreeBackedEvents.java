package com.graphaware.module.timetree;

import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.domain.TimeInstant;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.graphaware.module.timetree.SingleTimeTree.parent;
import static com.graphaware.module.timetree.domain.TimeTreeRelationshipTypes.*;
import static com.graphaware.module.timetree.domain.ValidationUtils.validateRange;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * {@link TimedEvents} backed by a {@link TimeTree}.
 */
public class TimeTreeBackedEvents implements TimedEvents {

    private final TimeTree timeTree;

    private static final List<String> timeTreeRelationships = getTimeTreeRelationshipNames();

    public TimeTreeBackedEvents(TimeTree timeTree) {
        this.timeTree = timeTree;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean attachEvent(Node event, RelationshipType relationshipType, TimeInstant timeInstant) {
        Node instant = timeTree.getOrCreateInstant(timeInstant);

        for (Relationship existing : event.getRelationships(OUTGOING, relationshipType)) {
            if (existing.getEndNode().equals(instant)) {
                return false;
            }
        }

        event.createRelationshipTo(instant, relationshipType);
        return true;
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
        validateRange(startTime, endTime);

        List<Event> events = new LinkedList<>();

        Node startTimeNode = timeTree.getInstantAtOrAfter(startTime);
        Node endTimeNode = timeTree.getInstantAtOrBefore(endTime);

        if (startTimeNode == null || endTimeNode == null) {
            return events;
        }

        events.addAll(getEventsAttachedToNodeAndChildren(startTimeNode, type));

        if (startTimeNode.equals(endTimeNode)) {
            return events;
        }

        Relationship next = startTimeNode.getSingleRelationship(NEXT, OUTGOING);
        while (next != null && !(next.getEndNode().equals(endTimeNode))) {
            Node timeInstant = next.getEndNode();
            events.addAll(getEventsAttachedToNodeAndChildren(timeInstant, type));
            next = timeInstant.getSingleRelationship(NEXT, OUTGOING);
        }
        events.addAll(getEventsAttachedToNodeAndChildren(endTimeNode, type));

        return events;
    }

    private List<Event> getEventsAttachedToNodeAndChildren(Node parent, RelationshipType type) {
        List<Event> result = new ArrayList<>();

        Relationship firstRelationship = parent.getSingleRelationship(FIRST, OUTGOING);
        if (firstRelationship == null) {
            return getEventsAttachedToNode(parent, type);
        }

        Node child = null;

        while (true) {
            if (child == null) {
                child = firstRelationship.getEndNode();
            } else {
                Relationship nextRelationship = child.getSingleRelationship(NEXT, OUTGOING);

                if (nextRelationship == null || parent(nextRelationship.getEndNode()).getId() != parent.getId()) {
                    break;
                }

                child = nextRelationship.getEndNode();
            }

            result.addAll(getEventsAttachedToNodeAndChildren(child, type));
        }

        result.addAll(getEventsAttachedToNode(parent, type));

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
