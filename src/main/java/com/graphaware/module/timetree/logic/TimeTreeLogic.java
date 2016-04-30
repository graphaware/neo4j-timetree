/*
 * Copyright (c) 2013-2016 GraphAware
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
package com.graphaware.module.timetree.logic;

import com.graphaware.module.timetree.SingleTimeTree;
import com.graphaware.module.timetree.TimeTree;
import com.graphaware.module.timetree.api.TimeInstantVO;
import com.graphaware.module.timetree.domain.TimeInstant;
import java.util.List;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;

public class TimeTreeLogic {

    private final GraphDatabaseService database;
    private final TimeTree timeTree;

    public TimeTreeLogic(GraphDatabaseService database) {
        this.database = database;
        this.timeTree = new SingleTimeTree(database);
    }

    public Node getInstant(long time, String resolution, String timezone) throws NotFoundException {
        Node instant;
        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));
        try (Transaction tx = database.beginTx()) {
            instant = timeTree.getInstant(timeInstant);
            tx.success();
        }
        if (instant == null) {
            throw new NotFoundException("There is no time instant for time " + time);
        }
        return instant;
    }

    public Node getOrCreateInstant(long time, String resolution, String timezone) {
        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));
        Node id;
        try (Transaction tx = database.beginTx()) {
            id = timeTree.getOrCreateInstant(timeInstant);
            tx.success();
        }
        return id;
    }

    public List<Node> getInstants(long startTime, long endTime, String resolution, String timezone) {
        TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
        TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));
        List<Node> nodes;
        try (Transaction tx = database.beginTx()) {
            nodes = timeTree.getInstants(startTimeInstant, endTimeInstant);
            tx.success();
        }
        return nodes;
    }

}
