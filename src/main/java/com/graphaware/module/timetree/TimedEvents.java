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

package com.graphaware.module.timetree;

import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.domain.TimeInstant;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;
import java.util.Set;

/**
 * API for representing events in time.
 */
public interface TimedEvents {

    /**
     * Attach an event to a node representing a specific time instant, using an incoming relationship
     * (from the time instant's point of view) of the specified type. If the time instant doesn't exist, it will be created.
     *
     * @param event            event node to be associated with the specified time instant.
     * @param relationshipType type of the relationship between the event node and the time instant node.
     * @param timeInstant      specific time instant to attach the event to.
     * @return <code>true</code> iff the event was attached, <code>false</code> iff it was already attached.
     */
    boolean attachEvent(Node event, RelationshipType relationshipType, TimeInstant timeInstant);

    /**
     * Attach an event to a node representing a specific time instant, using a relationship of the specified direction
     * (from the time instant's point of view) and the specified type. If the time instant doesn't exist, it will be created.
     *
     * @param event            event node to be associated with the specified time instant.
     * @param relationshipType type of the relationship between the event node and the time instant node.
     * @param direction        of the relationship between the time instant and the event from the time instant's point of view. Must not be {@link Direction#BOTH}.
     * @param timeInstant      specific time instant to attach the event to.
     * @return <code>true</code> iff the event was attached, <code>false</code> iff it was already attached.
     */
    boolean attachEvent(Node event, RelationshipType relationshipType, Direction direction, TimeInstant timeInstant);

    /**
     * Get events attached (using any incoming relationship) to a specific time instant and all its children.
     * If the time instant doesn't exist, it will <b>not</b> be created and an empty list will be returned.
     *
     * @param timeInstant specific time instant.
     * @return events attached to the time instant and all children. Ordered chronologically with events with higher
     * resolution before events with lower resolution.
     */
    List<Event> getEvents(TimeInstant timeInstant);

    /**
     * Get events attached (using any relationship of the specified direction) to a specific time instant and all its children.
     * If the time instant doesn't exist, it will <b>not</b> be created and an empty list will be returned.
     *
     * @param timeInstant specific time instant.
     * @param direction   of the relationships between the time instant and the events from the time instant's point of view.
     * @return events attached to the time instant and all children. Ordered chronologically with events with higher
     * resolution before events with lower resolution.
     */
    List<Event> getEvents(TimeInstant timeInstant, Direction direction);

    /**
     * Get events attached (via any incoming relationship) to all time instants in the specified range (inclusive) and
     * all their children. The time instants that don't exist will <b>not</b> be created.
     *
     * @param startTime Time instant representing the start of the interval (inclusive).
     * @param endTime   Time instant representing the end of the interval (inclusive).
     * @return events attached to all time instants in the interval and their children. Ordered chronologically with events with higher
     * resolution before events with lower resolution.
     */
    List<Event> getEvents(TimeInstant startTime, TimeInstant endTime);

    /**
     * Get events attached (via any relationship of the specified direction) to all time instants in the specified range (inclusive) and
     * all their children. The time instants that don't exist will <b>not</b> be created.
     *
     * @param startTime Time instant representing the start of the interval (inclusive).
     * @param endTime   Time instant representing the end of the interval (inclusive).
     * @param direction of the relationships between the time instants and the events from the time instants' point of view.
     * @return events attached to all time instants in the interval and their children. Ordered chronologically with events with higher
     * resolution before events with lower resolution.
     */
    List<Event> getEvents(TimeInstant startTime, TimeInstant endTime, Direction direction);

    /**
     * Get events attached (via an incoming relationship of one of the specified types) to a specific time instant and all its children.
     * If the time instant doesn't exist, it will <b>not</b> be created and an empty list will be returned.
     *
     * @param timeInstant       specific time instant.
     * @param relationshipTypes of the relationships between the event and the time instant.
     * @return events attached to the time instant and all children. Ordered chronologically with events with higher
     * resolution before events with lower resolution.
     */
    List<Event> getEvents(TimeInstant timeInstant, Set<RelationshipType> relationshipTypes);

    /**
     * Get events attached (via a relationship of one of the specified types and direction) to a specific time instant and all its children.
     * If the time instant doesn't exist, it will <b>not</b> be created and an empty list will be returned.
     *
     * @param timeInstant       specific time instant.
     * @param relationshipTypes of the relationships between the event and the time instant.
     * @param direction         of the relationships between the time instant and the events from the time instant's point of view.
     * @return events attached to the time instant and all children. Ordered chronologically with events with higher
     * resolution before events with lower resolution.
     */
    List<Event> getEvents(TimeInstant timeInstant, Set<RelationshipType> relationshipTypes, Direction direction);

    /**
     * Get events attached (via an incoming relationship of one of the specified types) to all time instants in the specified range (inclusive) and
     * all their children. The time instants that don't exist will <b>not</b> be created.
     *
     * @param startTime         Time instant representing the start of the interval (inclusive).
     * @param endTime           Time instant representing the end of the interval (inclusive).
     * @param relationshipTypes of the relationships between the event and the time instants.
     * @return events attached to all time instants in the interval and their children. Ordered chronologically with events with higher
     * resolution before events with lower resolution.
     */
    List<Event> getEvents(TimeInstant startTime, TimeInstant endTime, Set<RelationshipType> relationshipTypes);

    /**
     * Get events attached (via a relationship of one of the specified types and the scpecified direction) to all time
     * instants in the specified range (inclusive) and all their children.
     * The time instants that don't exist will <b>not</b> be created.
     *
     * @param startTime         Time instant representing the start of the interval (inclusive).
     * @param endTime           Time instant representing the end of the interval (inclusive).
     * @param relationshipTypes of the relationships between the event and the time instants.
     * @param direction         of the relationships between the time instants and the events from the time instants' point of view.
     * @return events attached to all time instants in the interval and their children. Ordered chronologically with events with higher
     * resolution before events with lower resolution.
     */
    List<Event> getEvents(TimeInstant startTime, TimeInstant endTime, Set<RelationshipType> relationshipTypes, Direction direction);
}
