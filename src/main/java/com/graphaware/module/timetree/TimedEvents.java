/*
 * Copyright (c) 2014 GraphAware
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

import java.util.List;

/**
 * API for representing events in time.
 */
public interface TimedEvents {

    /**
     * Attach an event to a node representing a specific time instant. If the time instant doesn't exist, it will be created.
     *
     * @param event            event node to be associated with the specified time instant.
     * @param relationshipType type of the relationship between the event node and the time instant node.
     * @param timeInstant      specific time instant to attach the event to.
     */
    void attachEvent(Node event, RelationshipType relationshipType, TimeInstant timeInstant);

    /**
     * Get events attached to a specific time instant. If the time instant doesn't exist, it will <b>not</b> be created
     * and an empty list will be returned.
     *
     * @param timeInstant specific time instant.
     * @return events attached to the time instant.
     */
    List<Event> getEvents(TimeInstant timeInstant);

    /**
     * Get events attached to all time instants in the specified range (inclusive). The time instants that don't exist will be created.
     * <p/>
     * <p>
     * Note that this may throw an org.neo4j.graphdb.NotFoundException if the root of the single time tree has been deleted.
     * If this occurs, call invalidateCaches() and retry this method.
     * </p>
     *
     * @param startTime TimeInstant representing the start of the interval (inclusive)
     * @param endTime   TimeInstant representing the end of the interval (inclusive)
     * @return events attached to all time instants in the interval, ordered chronologically
     */
    List<Event> getEvents(TimeInstant startTime, TimeInstant endTime);

    /**
     * Get events attached to a specific time instant with a specific relation. If the time instant doesn't exist, it will be created.
     * <p/>
     * <p>
     * Note that this may throw an org.neo4j.graphdb.NotFoundException if the root of the single time tree has been deleted.
     * If this occurs, call invalidateCaches() and retry this method.
     * </p>
     *
     * @param timeInstant      specific TimeInstant
     * @param relationshipType relationship attaching the event to the timeInstant
     * @return events attached to the time instant with the specified relation
     */
    List<Event> getEvents(TimeInstant timeInstant, RelationshipType relationshipType);

    /**
     * Get events attached to all time instants with the specified relation, in the specified range (inclusive). The time instants that don't exist will be created.
     * <p/>
     * <p>
     * Note that this may throw an org.neo4j.graphdb.NotFoundException if the root of the single time tree has been deleted.
     * If this occurs, call invalidateCaches() and retry this method.
     * </p>
     *
     * @param startTime        TimeInstant representing the start of the interval (inclusive)
     * @param endTime          TimeInstant representing the end of the interval (inclusive)
     * @param relationshipType relationship attaching the event to the timeInstant
     * @return events attached to all time instants in the interval with the specified relation, ordered chronologically
     */
    List<Event> getEvents(TimeInstant startTime, TimeInstant endTime, RelationshipType relationshipType);
}
