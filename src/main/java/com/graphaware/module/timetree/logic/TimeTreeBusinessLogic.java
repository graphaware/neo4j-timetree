/*
 * Copyright (c) 2013-2017 GraphAware
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

import com.graphaware.module.timetree.CustomRootTimeTree;
import com.graphaware.module.timetree.SingleTimeTree;
import com.graphaware.module.timetree.TimeTree;
import com.graphaware.module.timetree.api.TimeInstantVO;
import com.graphaware.module.timetree.domain.TimeInstant;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;

import java.util.List;

public class TimeTreeBusinessLogic {

    private final GraphDatabaseService database;
	private TimeTree timeTree;

    public TimeTreeBusinessLogic(GraphDatabaseService database) {
        this.database = database;
        this.timeTree = new SingleTimeTree(database);
    }

    /**
     * Find the instant of time in the tree
     * READ-ONLY
     * @param time
     * @param resolution
     * @param timezone
     * @return
     * @throws NotFoundException if there isn't the instant
     */
    public Node getInstant(long time, String resolution, String timezone) throws NotFoundException {
        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));
        Node instant = timeTree.getInstant(timeInstant);
        
        if (instant == null) {
            throw new NotFoundException("There is no time instant for time " + time);
        }

        return instant;
    }

    /**
     * Find the instant of time in the tree or create it if doesn't exist
     * @param time
     * @param resolution
     * @param timezone
     * @return a instant of time in the tree
     */
    public Node getOrCreateInstant(long time, String resolution, String timezone) {
        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));
        Node instant;
        try (Transaction tx = database.beginTx()) {
            instant = timeTree.getOrCreateInstant(timeInstant);
            tx.success();
        }
        return instant;
    }

    /**
     * Find instants of time in the tree using the input time range
     * READ-ONLY
     * @param startTime
     * @param endTime
     * @param resolution
     * @param timezone
     * @return empty list if no instants exists
     */
    public List<Node> getInstants(long startTime, long endTime, String resolution, String timezone) {
        TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
        TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));
        return timeTree.getInstants(startTimeInstant, endTimeInstant);
    }

    /**
     * Find instants of time in the tree using the input time range or create them at the resolution level
     * @param startTime
     * @param endTime
     * @param resolution
     * @param timezone
     * @return
     */
    public List<Node> getOrCreateInstants(long startTime, long endTime, String resolution, String timezone) {
        TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
        TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));
        List<Node> nodes;
        try (Transaction tx = database.beginTx()) {
            nodes = timeTree.getOrCreateInstants(startTimeInstant, endTimeInstant);
            tx.success();
        }
        return nodes;
    }
    
    /**
     * Find the instant in the tree with the specific root-node
     * READ-ONLY
     * @see #getInstant(long, String, String) 
     * @param rootNodeId
     * @param time
     * @param resolution
     * @param timezone
     * @return
     * @throws NotFoundException
     */
    public Node getInstantWithCustomRoot(long rootNodeId, long time, String resolution, String timezone) throws NotFoundException {
		TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));
		Node instant = new CustomRootTimeTree(database.getNodeById(rootNodeId)).getInstant(timeInstant);

		if (instant == null) {
			throw new NotFoundException("There is no time instant for time " + time);
		}
		return instant;
    }
    
    /**
     * Find or create the instant in the tree with the specific root-node
     * @see #getInstantWithCustomRoot(long, long, String, String)
     * @param rootNodeId
     * @param time
     * @param resolution
     * @param timezone
     * @return
     */
    public Node getOrCreateInstantWithCustomRoot(long rootNodeId, long time, String resolution, String timezone) {
      TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));
      Node instant;
      try (Transaction tx = database.beginTx()) {
          instant = new CustomRootTimeTree(database.getNodeById(rootNodeId)).getOrCreateInstant(timeInstant);
          tx.success();
      }
        return instant;
    }
    
    /**
     * Find the instants in the range on the tree with the specific root-node
     * READ-ONLY
     * @see #getInstantWithCustomRoot(long, long, String, String)
     * @param rootNodeId
     * @param startTime
     * @param endTime
     * @param resolution
     * @param timezone
     * @return
     */
    public List<Node> getInstantsWithCustomRoot(long rootNodeId, long startTime, long endTime, String resolution, String timezone) {
      TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
      TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));
      List<Node> nodes = new CustomRootTimeTree(database.getNodeById(rootNodeId)).getInstants(startTimeInstant, endTimeInstant);
      return nodes;
    }
    
    /**
     * Find or create the instants in the range on the tree with the specific root-node
     * @see #getInstants(long, long, String, String)
     * @param rootNodeId
     * @param startTime
     * @param endTime
     * @param resolution
     * @param timezone
     * @return
     */
    public List<Node> getOrCreateInstantsWithCustomRoot(long rootNodeId, long startTime, long endTime, String resolution, String timezone) {
      TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
      TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));
      List<Node> nodes;
      try (Transaction tx = database.beginTx()) {
          nodes = new CustomRootTimeTree(database.getNodeById(rootNodeId)).getOrCreateInstants(startTimeInstant, endTimeInstant);
          tx.success();
      }
        return nodes;
    }

}
