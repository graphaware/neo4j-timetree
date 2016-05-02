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

import com.graphaware.module.timetree.CustomRootTimeTree;
import com.graphaware.module.timetree.TimeTreeBackedEvents;
import com.graphaware.module.timetree.TimedEvents;
import com.graphaware.module.timetree.api.TimeInstantVO;
import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.domain.TimeInstant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class TimedEventsBusinessLogic {

    private final GraphDatabaseService database;
    private final TimedEvents timedEvents;

    public TimedEventsBusinessLogic(GraphDatabaseService database, TimedEvents timedEvents) {
        this.database = database;
        this.timedEvents = timedEvents;
    }

    public List<Event> getEvents(long time, String resolution, String timezone, Set<String> relationshipTypes, String direction) {
        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));
        List<Event> events;
        try (Transaction tx = database.beginTx()) {
            events = timedEvents.getEvents(timeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
            tx.success();
        }
        return events;
    }
    
    public List<Event> getEvents(long startTime, String resolution, String timezone, long endTime, Set<String> relationshipTypes, String direction) {
      TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
      TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));
      List<Event> events;
      try (Transaction tx = database.beginTx()) {
          events = timedEvents.getEvents(startTimeInstant, endTimeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
          tx.success();
      }
        return events;
    }
    
    public List<Event> getEventsCustomRoot(long time, String resolution, String timezone, long rootNodeId, Set<String> relationshipTypes, String direction) {
      TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));
      List<Event> events;
      try (Transaction tx = database.beginTx()) {
          CustomRootTimeTree timeTree = new CustomRootTimeTree(database.getNodeById(rootNodeId));
          events = new TimeTreeBackedEvents(timeTree).getEvents(timeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
          tx.success();
      }
        return events;
    }
    
    public List<Event> getEventsCustomRoot(long startTime, String resolution, String timezone, long endTime, long rootNodeId, Set<String> relationshipTypes, String direction) {
      TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
      TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));
      List<Event> events;
      try (Transaction tx = database.beginTx()) {
          CustomRootTimeTree timeTree = new CustomRootTimeTree(database.getNodeById(rootNodeId));
          events = new TimeTreeBackedEvents(timeTree).getEvents(startTimeInstant, endTimeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
          tx.success();
      }
        return events;
    }

    private Set<RelationshipType> getRelationshipTypes(Set<String> strings) {
        if (strings == null) {
            return null;
        }

        Set<RelationshipType> result = new HashSet<>();
        for (String type : strings) {
            result.add(RelationshipType.withName(type));
        }
        return result;
    }
    
    private Direction resolveDirection(String direction) {
        if (direction == null) {
            return Direction.INCOMING;
        }

        return Direction.valueOf(direction.toUpperCase());
    }
}
