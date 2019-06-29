/*
 * Copyright (c) 2013-2019 GraphAware
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

import com.graphaware.module.timetree.domain.TimeInstant;
import org.neo4j.graphdb.Node;

import java.util.List;

/**
 * API for representing time as a tree (also called GraphAware TimeTree). Provides methods for creating and retrieving
 * nodes that represent instances of time, making sure that a tree of time is maintained with each created node.
 * There is no support for changes or deletes.
 */
public interface TimeTree {

    /**
     * Get a node representing a specific time instant. Return <code>null</code> if it does not exist.
     *
     * @param timeInstant specific time instant.
     * @return node representing a specific time instant, <code>null</code> if no such node exists.
     */
    Node getInstant(TimeInstant timeInstant);

    /**
     * Get a node representing a specific time instant or the first instance thereafter, if the specific time instant
     * does not exits. Return <code>null</code> if no such node exists.
     *
     * @param timeInstant specific time instant.
     * @return node representing a specific time instant or the first instant thereafter, <code>null</code> if no such node exists.
     */
    Node getInstantAtOrAfter(TimeInstant timeInstant);

    /**
     * Get a node representing a specific time instant or the first instance before that, if the specific time instant
     * does not exits. Return <code>null</code> if no such ndoe exists.
     *
     * @param timeInstant specific time instant.
     * @return node representing a specific time instant or the first instant before that, <code>null</code> if no such node exists.
     */
    Node getInstantAtOrBefore(TimeInstant timeInstant);

    /**
     * Get nodes representing all time instants in the specified range (inclusive).
     *
     * @param startTime Time instant representing the start of the interval (inclusive).
     * @param endTime   Time instant representing the end of the interval (inclusive).
     * @return nodes representing all time instants in the interval, ordered chronologically.
     */
    List<Node> getInstants(TimeInstant startTime, TimeInstant endTime);

    /**
     * Get a node representing a specific time instant. If one doesn't exist, it will be created.
     *
     * @param timeInstant specific TimeInstant
     * @return node representing a specific time instant.
     */
    Node getOrCreateInstant(TimeInstant timeInstant);

    /**
     * Get nodes representing all time instants in the specified range (inclusive). The ones that don't exist will be created.
     *
     * @param startTime TimeInstant representing the start of the interval (inclusive)
     * @param endTime   TimeInstant representing the end of the interval (inclusive)
     * @return nodes representing all time instants in the interval, ordered chronologically.
     */
    List<Node> getOrCreateInstants(TimeInstant startTime, TimeInstant endTime);

    /**
     * Remove the Complete Index-Tree.
     * <b>ATTENTION</b> this will remove all the root-node and nodes matching (root)-[:CHILD*1..]-(child) so never
     * link your events with a CHILD-relationship to the index.
     */
    void removeAll();

    /**
     * Remove a time instant, this instant must have no events attached and no child-nodes. If it does have any events
     * or child nodes attached, it will not be removed and a warning will be logged.
     *
     * @param instantNode finest Resolution TimeInstant
     */
    void removeInstant(Node instantNode);
}
