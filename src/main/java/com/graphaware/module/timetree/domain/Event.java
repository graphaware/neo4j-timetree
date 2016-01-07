/*
 * Copyright (c) 2013-2015 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.graphaware.module.timetree.domain;

import com.graphaware.api.json.LongIdJsonNode;
import com.graphaware.module.timetree.api.EventVO;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * An event intended to be attached to the {@link com.graphaware.module.timetree.TimeTree}.
 */
public class Event {

    private final Node node;
    private final RelationshipType relationshipType;
    private final Direction direction;

    /**
     * Create a new event.
     *
     * @param node             representing the event.
     * @param relationshipType to use when attaching the event to the time tree.
     * @param direction to use when attaching the event to the time tree.
     */
    public Event(Node node, RelationshipType relationshipType, Direction direction) {
        this.node = node;
        this.relationshipType = relationshipType;
        this.direction = direction;
    }

    /**
     * Get the node representing the event.
     *
     * @return event node.
     */
    public Node getNode() {
        return node;
    }

    /**
     * Get the relationship type between the event and the time instant.
     *
     * @return the relationship between the event and the time instant.
     */
    public RelationshipType getRelationshipType() {
        return relationshipType;
    }

    /**
     * Get the direction of the relationship between the time instant and the event from the time instant's point of view.
     *
     * @return the relationship direction between the time instant and the event.
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * Convert this event to its corresponding value object.
     *
     * @return value object.
     */
    public EventVO toValueObject() {
        return new EventVO(new LongIdJsonNode(node), getRelationshipType().name(), getDirection().name());
    }
}
