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

package com.graphaware.module.timetree.api;

import com.graphaware.api.JsonNode;
import com.graphaware.module.timetree.CustomRootTimeTree;
import com.graphaware.module.timetree.TimeTreeBackedEvents;
import com.graphaware.module.timetree.TimedEvents;
import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.domain.TimeInstant;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.util.*;

/**
 * REST API for {@link TimedEvents}.
 */
@Controller
@RequestMapping("/timetree")
public class TimedEventsApi {

    private static final Logger LOG = LoggerFactory.getLogger(TimedEventsApi.class);

    private final GraphDatabaseService database;
    private final TimedEvents timedEvents;

    @Autowired
    public TimedEventsApi(GraphDatabaseService database, TimedEvents timedEvents) {
        this.database = database;
        this.timedEvents = timedEvents;
    }

    @RequestMapping(value = "/single/{time}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<EventVO> getEvents(
            @PathVariable long time,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone,
            @RequestParam(required = false) Set<String> relationshipTypes,
            @RequestParam(required = false) String direction) {

        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));

        List<Event> events;
        try (Transaction tx = database.beginTx()) {
            events = timedEvents.getEvents(timeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
            tx.success();
        }

        List<EventVO> result;
        try (Transaction tx = database.beginTx()) {
            result = convertEvents(events);
            tx.success();
        }

        return result;
    }

    @RequestMapping(value = "/single/event", method = RequestMethod.POST)
    @ResponseBody
    public JsonNode attachEvent(@RequestBody TimedEventVO event, HttpServletResponse response) {
        event.validate();

        long id;
        try (Transaction tx = database.beginTx()) {
            Node eventNode = event.getEvent().getNode().producePropertyContainer(database);
            id = eventNode.getId();

            boolean attached = timedEvents.attachEvent(
                    eventNode,
                    DynamicRelationshipType.withName(event.getEvent().getRelationshipType()),
                    resolveDirection(event.getEvent().getDirection()),
                    TimeInstant.fromValueObject(event.getTimeInstant()));

            if (attached) {
                response.setStatus(HttpStatus.CREATED.value());
            } else {
                response.setStatus(HttpStatus.OK.value());
            }

            tx.success();
        }

        JsonNode result;
        try (Transaction tx = database.beginTx()) {
            result = new JsonNode(database.getNodeById(id));
            tx.success();
        }

        return result;
    }

    @RequestMapping(value = "/range/{startTime}/{endTime}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<EventVO> getEvents(
            @PathVariable long startTime,
            @PathVariable long endTime,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone,
            @RequestParam(required = false) Set<String> relationshipTypes,
            @RequestParam(required = false) String direction) {


        TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
        TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));

        List<Event> events;
        try (Transaction tx = database.beginTx()) {
            events = timedEvents.getEvents(startTimeInstant, endTimeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
            tx.success();
        }

        List<EventVO> result;
        try (Transaction tx = database.beginTx()) {
            result = convertEvents(events);
            tx.success();
        }

        return result;
    }


    @RequestMapping(value = "/{rootNodeId}/single/{time}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<EventVO> getEventsCustomRoot(
            @PathVariable long rootNodeId,
            @PathVariable long time,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone,
            @RequestParam(required = false) Set<String> relationshipTypes,
            @RequestParam(required = false) String direction) {


        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));

        List<Event> events;
        try (Transaction tx = database.beginTx()) {
            CustomRootTimeTree timeTree = new CustomRootTimeTree(database.getNodeById(rootNodeId));
            events = new TimeTreeBackedEvents(timeTree).getEvents(timeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
            tx.success();
        }

        List<EventVO> result;
        try (Transaction tx = database.beginTx()) {
            result = convertEvents(events);
            tx.success();
        }

        return result;
    }

    @RequestMapping(value = "/{rootNodeId}/range/{startTime}/{endTime}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<EventVO> getEventsCustomRoot(
            @PathVariable long rootNodeId,
            @PathVariable long startTime,
            @PathVariable long endTime,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone,
            @RequestParam(required = false) Set<String> relationshipTypes,
            @RequestParam(required = false) String direction) {

        TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
        TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));

        List<Event> events;
        try (Transaction tx = database.beginTx()) {
            CustomRootTimeTree timeTree = new CustomRootTimeTree(database.getNodeById(rootNodeId));
            events = new TimeTreeBackedEvents(timeTree).getEvents(startTimeInstant, endTimeInstant, getRelationshipTypes(relationshipTypes), resolveDirection(direction));
            tx.success();
        }

        List<EventVO> result;
        try (Transaction tx = database.beginTx()) {
            result = convertEvents(events);
            tx.success();
        }

        return result;
    }

    @RequestMapping(value = "{rootNodeId}/single/event", method = RequestMethod.POST)
    @ResponseBody
    public JsonNode attachEvent(@RequestBody TimedEventVO event, @PathVariable long rootNodeId, HttpServletResponse response) {
        event.validate();

        long id;
        try (Transaction tx = database.beginTx()) {
            Node eventNode = event.getEvent().getNode().producePropertyContainer(database);
            id = eventNode.getId();

            CustomRootTimeTree timeTree = new CustomRootTimeTree(database.getNodeById(rootNodeId));

            boolean attached = new TimeTreeBackedEvents(timeTree).attachEvent(
                    eventNode,
                    DynamicRelationshipType.withName(event.getEvent().getRelationshipType()),
                    resolveDirection(event.getEvent().getDirection()),
                    TimeInstant.fromValueObject(event.getTimeInstant()));

            if (attached) {
                response.setStatus(HttpStatus.CREATED.value());
            } else {
                response.setStatus(HttpStatus.OK.value());
            }

            tx.success();
        }

        JsonNode result;
        try (Transaction tx = database.beginTx()) {
            result = new JsonNode(database.getNodeById(id));
            tx.success();
        }

        return result;
    }

    private Set<RelationshipType> getRelationshipTypes(Set<String> strings) {
        if (strings == null) {
            return null;
        }

        Set<RelationshipType> result = new HashSet<>();
        for (String type : strings) {
            result.add(DynamicRelationshipType.withName(type));
        }
        return result;
    }

    private List<EventVO> convertEvents(List<Event> events) {
        List<EventVO> eventVOs = new ArrayList<>(events.size());
        for (Event event : events) {
            eventVOs.add(event.toValueObject());
        }
        return eventVOs;
    }

    private Direction resolveDirection(String direction) {
        if (direction == null) {
            return Direction.INCOMING;
        }

        return Direction.valueOf(direction.toUpperCase());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    public Map<String, String> handleIllegalArgument(IllegalArgumentException e) {
        LOG.warn("Bad Request: " + e.getMessage(), e);
        return Collections.singletonMap("message", e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    public Map<String, String> handleIllegalState(IllegalStateException e) {
        LOG.warn("Bad Request: " + e.getMessage(), e);
        return Collections.singletonMap("message", e.getMessage());
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ResponseBody
    public Map<String, String> handleNotFound(NotFoundException e) {
        LOG.warn("Not Found: " + e.getMessage(), e);
        return Collections.singletonMap("message", e.getMessage());
    }
}
