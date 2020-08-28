/*
 * Copyright (c) 2013-2020 GraphAware
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
import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.domain.TimeInstant;
import org.neo4j.graphdb.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TimedEventsBusinessLogic {

    private final GraphDatabaseService database;
    private final TimedEvents timedEvents;

    public TimedEventsBusinessLogic(GraphDatabaseService database, TimedEvents timedEvents) {
        this.database = database;
        this.timedEvents = timedEvents;
    }

    public List<Event> getEvents(long time, String resolution, String timezone, Collection<String> relationshipTypes, String direction) {
        TimeInstant timeInstant = TimeInstant.createInstant(time, resolution, timezone);
        List<Event> events;
        try (Transaction tx = database.beginTx()) {
            events = timedEvents.getEvents(timeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
            tx.success();
        }
        return events;
    }
    
    public List<Event> getEvents(long startTime, long endTime, String resolution, String timezone, Collection<String> relationshipTypes, String direction) {
      TimeInstant startTimeInstant = TimeInstant.createInstant(startTime, resolution, timezone);
      TimeInstant endTimeInstant = TimeInstant.createInstant(endTime, resolution, timezone);
      List<Event> events;
      try (Transaction tx = database.beginTx()) {
          events = timedEvents.getEvents(startTimeInstant, endTimeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
          tx.success();
      }
        return events;
    }
    
    public List<Event> getEventsCustomRoot(long rootNodeId, long time, String resolution, String timezone, Collection<String> relationshipTypes, String direction) {
      TimeInstant timeInstant = TimeInstant.createInstant(time, resolution, timezone);
      List<Event> events;
      try (Transaction tx = database.beginTx()) {
          CustomRootTimeTree timeTree = new CustomRootTimeTree(database.getNodeById(rootNodeId));
          events = new TimeTreeBackedEvents(timeTree).getEvents(timeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
          tx.success();
      }
        return events;
    }
    
    public List<Event> getEventsCustomRoot(long rootNodeId, long startTime, long endTime, String resolution, String timezone, Collection<String> relationshipTypes, String direction) {
      TimeInstant startTimeInstant = TimeInstant.createInstant(startTime, resolution, timezone);
      TimeInstant endTimeInstant = TimeInstant.createInstant(endTime, resolution, timezone);
      List<Event> events;
      try (Transaction tx = database.beginTx()) {
          CustomRootTimeTree timeTree = new CustomRootTimeTree(database.getNodeById(rootNodeId));
          events = new TimeTreeBackedEvents(timeTree).getEvents(startTimeInstant, endTimeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
          tx.success();
      }
        return events;
    }
    
    public boolean attachEvent(Node eventNode, 
            RelationshipType relationshipType, 
            String direction, 
            long time, 
            String timezone, 
            String resolution) {
        boolean attached;
        try (Transaction tx = database.beginTx()) {
            attached = timedEvents.attachEvent(eventNode,
                    relationshipType,
                    resolveDirection(direction),
                    TimeInstant.createInstant(time, resolution, timezone));
            tx.success();
        }
        return attached;
    }

    public boolean attachEventWithCustomRoot(
            Node root,
            Node eventNode,
            RelationshipType relationshipType,
            String direction,
            long time,
            String timezone,
            String resolution) {

        boolean attached;
        try (Transaction tx = database.beginTx()) {
            CustomRootTimeTree timeTree = new CustomRootTimeTree(root);
            TimedEvents customTimedEvents = new TimeTreeBackedEvents(timeTree);
            attached = customTimedEvents.attachEvent(
                    eventNode,
                    relationshipType,
                    resolveDirection(direction),
                    TimeInstant.createInstant(time, resolution, timezone)
            );
            tx.success();
        }

        return attached;
    }

    private Set<RelationshipType> getRelationshipTypes(Collection<String> strings) {
        if (strings == null) {
            return null;
        }

        Set<RelationshipType> result = new HashSet<>();
        for (String type : strings) {
            if (type != null)
                result.add(RelationshipType.withName(type));
        }
        return result.size() > 0 ? result : null;
    }
    
    private Direction resolveDirection(String direction) {
        if (direction == null) {
            return Direction.INCOMING;
        }

        return Direction.valueOf(direction.toUpperCase());
    }
    
    public class EventAttachedResult {

        private long id = -1;
        private boolean attached = false;

        public EventAttachedResult(long id, boolean attached) {
            this.id = id;
            this.attached = attached;
        }        
        
        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public boolean isAttached() {
            return attached;
        }

        public void setAttached(boolean attached) {
            this.attached = attached;
        }
        
    }
}
