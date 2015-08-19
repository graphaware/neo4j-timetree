package com.graphaware.module.timetree;

import com.graphaware.common.util.DirectionUtils;
import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.domain.TimeInstant;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.*;

import static com.graphaware.module.timetree.SingleTimeTree.parent;
import static com.graphaware.module.timetree.domain.TimeTreeRelationshipTypes.*;
import static com.graphaware.module.timetree.domain.ValidationUtils.validateRange;
import static org.neo4j.graphdb.Direction.BOTH;
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
        return attachEvent(event, relationshipType, INCOMING, timeInstant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean attachEvent(Node event, RelationshipType relationshipType, Direction direction, TimeInstant timeInstant) {
        if (!INCOMING.equals(direction) && !OUTGOING.equals(direction)) {
            throw new IllegalArgumentException("Direction must be INCOMING or OUTGOING!");
        }

        Node instant = timeTree.getOrCreateInstant(timeInstant);

        for (Relationship existing : event.getRelationships(DirectionUtils.reverse(direction), relationshipType)) {
            if (existing.getEndNode().getId() == instant.getId()) {
                return false;
            }
        }

        if (INCOMING.equals(direction)) {
            event.createRelationshipTo(instant, relationshipType);
            return true;
        }

        if (OUTGOING.equals(direction)) {
            instant.createRelationshipTo(event, relationshipType);
            return true;
        }

        throw new IllegalStateException("This must never happen - it is a bug");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant timeInstant) {
        return getEvents(timeInstant, INCOMING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant timeInstant, Direction direction) {
        return getEvents(timeInstant, (Set<RelationshipType>) null, direction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant startTime, TimeInstant endTime) {
        return getEvents(startTime, endTime, INCOMING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant startTime, TimeInstant endTime, Direction direction) {
        return getEvents(startTime, endTime, null, INCOMING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant timeInstant, Set<RelationshipType> types) {
        return getEvents(timeInstant, types, INCOMING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant timeInstant, Set<RelationshipType> types, Direction direction) {
        Node instantNode = timeTree.getInstant(timeInstant);

        if (instantNode == null) {
            return Collections.emptyList();
        }

        return getEventsAttachedToNodeAndChildren(instantNode, types, direction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant startTime, TimeInstant endTime, Set<RelationshipType> relationshipTypes) {
        return getEvents(startTime, endTime, relationshipTypes, INCOMING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(TimeInstant startTime, TimeInstant endTime, Set<RelationshipType> types, Direction direction) {
        validateRange(startTime, endTime);

        List<Event> events = new LinkedList<>();

        Node startTimeNode = timeTree.getInstantAtOrAfter(startTime);
        Node endTimeNode = timeTree.getInstantAtOrBefore(endTime);

        if (startTimeNode == null || endTimeNode == null) {
            return events;
        }

        events.addAll(getEventsAttachedToNodeAndChildren(startTimeNode, types, direction));

        if (startTimeNode.getId() == endTimeNode.getId()) {
            return events;
        }

        Relationship next = startTimeNode.getSingleRelationship(NEXT, OUTGOING);
        while (next != null && !(next.getEndNode().equals(endTimeNode))) {
            Node timeInstant = next.getEndNode();
            events.addAll(getEventsAttachedToNodeAndChildren(timeInstant, types, direction));
            next = timeInstant.getSingleRelationship(NEXT, OUTGOING);
        }
        events.addAll(getEventsAttachedToNodeAndChildren(endTimeNode, types, direction));

        return events;
    }

    private List<Event> getEventsAttachedToNodeAndChildren(Node parent, Set<RelationshipType> types, Direction direction) {
        List<Event> result = new ArrayList<>();

        Relationship firstRelationship = parent.getSingleRelationship(FIRST, OUTGOING);
        if (firstRelationship == null) {
            return getEventsAttachedToNode(parent, types, direction);
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

            result.addAll(getEventsAttachedToNodeAndChildren(child, types, direction));
        }

        result.addAll(getEventsAttachedToNode(parent, types, direction));

        return result;
    }

    private List<Event> getEventsAttachedToNode(Node node, Set<RelationshipType> types, Direction direction) {
        List<Event> result = new LinkedList<>();

        for (Relationship rel : node.getRelationships(direction)) {
            if (!timeTreeRelationships.contains(rel.getType().name())) {
                if (types == null || contains(types, rel.getType())) {
                    result.add(new Event(rel.getOtherNode(node), rel.getType(), DirectionUtils.resolveDirection(rel, node)));
                }
            }
        }

        return result;
    }

    private boolean contains(Set<RelationshipType> types, RelationshipType toCheck) {
        if (types == null || toCheck == null) {
            throw new IllegalArgumentException("Relationship types must not be null, this is a bug");
        }

        for (RelationshipType type : types) {
            if (toCheck.name().equals(type.name())) {
                return true;
            }
        }

        return false;
    }
}
