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

import org.joda.time.DateTimeZone;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.List;

/**
 * API for representing time as a tree (also called GraphAware TimeTree). Provides methods for creating and retrieving
 * nodes that represent instances of time, making sure that a tree of time is maintained with each created node.
 * There is no support for changes or deletes.
 */
public interface TimeTree {

    /**
     * Get a node representing this time instant. If one doesn't exist, it will be created.
     * <p/>
     * The resolution of the time instant (i.e., whether it is a day, hour, minute, etc.) depends on the implementation,
     * which can choose a sensible default, require to be configured with a default when instantiated, or both.
     * <p/>
     * The time zone of the time instant depends on the implementation, which can choose a default, require to be
     * configured with a default when instantiated, or both.
     *
     * @param tx currently running transaction.
     * @return node representing the time instant when this method was called.
     */
    Node getNow(Transaction tx);


    /**
     * Get a node representing this time instant. If one doesn't exist, it will be created.
     *
     * @param timeInstant specific TimeInstant
     * @param tx          currently running transaction.
     * @return node representing the time instant
     */
    Node getNow(TimeInstant timeInstant, Transaction tx);

    /**
     * Get a node representing a specific time instant. If one doesn't exist, it will be created.
     *
     * @param timeInstant specific TimeInstant
     * @param tx          currently running transaction.
     * @return node representing a specific time instant.
     */
    Node getInstant(TimeInstant timeInstant, Transaction tx);

    /**
     * Get nodes representing all time instants in the specified range (inclusive). The ones that don't exist will be created.
     * @param startTime TimeInstant representing the start of the interval (inclusive)
     * @param endTime   TimeInstant representing the end of the interval (inclusive)
     * @param tx        currently running transaction.
     * @return nodes representing all time instants in the interval, ordered chronologically.
     */
    List<Node> getInstants(TimeInstant startTime, TimeInstant endTime, Transaction tx);
}
