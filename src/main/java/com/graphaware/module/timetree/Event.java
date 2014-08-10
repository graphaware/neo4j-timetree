/*
 * Copyright (c) 2013 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.graphaware.module.timetree;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * An Event
 */
public class Event {

    private TimeInstant timeInstant;
    private Node eventNode;
    private RelationshipType eventRelation;

    /**
     * Get the time instant to which this event is attached
     *
     * @return the TimeInstant
     */
    public TimeInstant getTimeInstant() {
        return timeInstant;
    }

    /**
     * Sets the TimeInstant to which this event is attached
     *
     * @param timeInstant specific TimeInstant
     */
    public void setTimeInstant(TimeInstant timeInstant) {
        this.timeInstant = timeInstant;
    }

    /**
     * Get the node representing the event
     *
     * @return event node
     */
    public Node getEventNode() {
        return eventNode;
    }

    /**
     * Set the node representing the event
     *
     * @param eventNode the event node
     */
    public void setEventNode(Node eventNode) {
        this.eventNode = eventNode;
    }

    /**
     * Get the relationship that between the event and the time instant
     *
     * @return the relationship between the event and the time instant
     */
    public RelationshipType getEventRelation() {
        return eventRelation;
    }

    /**
     * Set the relationship between the event and time instant
     *
     * @param eventRelation
     */
    public void setEventRelation(RelationshipType eventRelation) {
        this.eventRelation = eventRelation;
    }


}
