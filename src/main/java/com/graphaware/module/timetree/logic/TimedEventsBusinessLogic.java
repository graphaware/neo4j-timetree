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
import com.graphaware.module.timetree.api.TimedEventVO;
import com.graphaware.module.timetree.api.TimedEventsApi;
import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.domain.TimeInstant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class TimedEventsBusinessLogic {

    private final GraphDatabaseService database;
    private final TimedEvents timedEvents;

    public TimedEventsBusinessLogic(GraphDatabaseService database, TimedEvents timedEvents) {
        this.database = database;
        this.timedEvents = timedEvents;
    }

    public List<Event> getEvents(long time, String resolution, String timezone, Collection<String> relationshipTypes, String direction) {
        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));
        List<Event> events;
        try (Transaction tx = database.beginTx()) {
            events = timedEvents.getEvents(timeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
            tx.success();
        }
        return events;
    }
    
    public List<Event> getEvents(long startTime, long endTime, String resolution, String timezone, Collection<String> relationshipTypes, String direction) {
      TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
      TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));
      List<Event> events;
      try (Transaction tx = database.beginTx()) {
          events = timedEvents.getEvents(startTimeInstant, endTimeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
          tx.success();
      }
        return events;
    }
    
    public List<Event> getEventsCustomRoot(long rootNodeId, long time, String resolution, String timezone, Collection<String> relationshipTypes, String direction) {
      TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));
      List<Event> events;
      try (Transaction tx = database.beginTx()) {
          CustomRootTimeTree timeTree = new CustomRootTimeTree(database.getNodeById(rootNodeId));
          events = new TimeTreeBackedEvents(timeTree).getEvents(timeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
          tx.success();
      }
        return events;
    }
    
    public List<Event> getEventsCustomRoot(long rootNodeId, long startTime, long endTime, String resolution, String timezone, Collection<String> relationshipTypes, String direction) {
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
    
    public EventAttachedResult attachEvent(TimedEventVO event) {
        EventAttachedResult res;
        event.validate();
        try (Transaction tx = database.beginTx()) {
            Node eventNode = event.getEvent().getNode().producePropertyContainer(database);
            long id = eventNode.getId();
            boolean attached = timedEvents.attachEvent(
                    eventNode,
                    RelationshipType.withName(event.getEvent().getRelationshipType()),
                    resolveDirection(event.getEvent().getDirection()),
                    TimeInstant.fromValueObject(event.getTimeInstant()));
            res  = new EventAttachedResult(id, attached);
            tx.success();
        }
        return res;
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
                    TimeInstant.createInstant(time, timezone, resolution));
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
                    TimeInstant.createInstant(time, timezone, resolution)
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
