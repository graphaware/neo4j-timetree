/*
 * Copyright (c) 2013 GraphAware
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

package com.graphaware.module.timetree.api;

import com.graphaware.module.timetree.*;
import org.neo4j.graphdb.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * REST API for {@link com.graphaware.module.timetree.TimeTree}.
 */
@Controller
@RequestMapping("/timetree")
public class EventApi {

    private final GraphDatabaseService database;
    private final TimeTree timeTree;
    private final TimedEvents timedEvents;

    @Autowired
    public EventApi(GraphDatabaseService database) {
        this.database = database;
        this.timeTree = new SingleTimeTree(database);
        this.timedEvents = new TimeTreeTimedEvents(timeTree);
    }

    @RequestMapping(value = "/single/{time}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<EventVO> getEventsAtInstant(
            @PathVariable long time,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone,
            @RequestParam(required = false) String relationshipType) {

        List<EventVO> events;

        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));

        try (Transaction tx = database.beginTx()) {
            events = convertEvents(timedEvents.getEvents(timeInstant, getRelationshipType(relationshipType)));
            tx.success();
        }

        return events;
    }

    @RequestMapping(value = "/single/event", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.CREATED)
    public void attachEventToInstant(@RequestBody EventVO event) {
        event.validate();

        try (Transaction tx = database.beginTx()) {
            Node eventNode = database.getNodeById(event.getNodeId());

            timedEvents.attachEvent(
                    eventNode,
                    DynamicRelationshipType.withName(event.getRelationshipType()),
                    TimeInstant.fromValueObject(event.getTimeInstant()));

            tx.success();
        }
    }


    @RequestMapping(value = "/range/{startTime}/{endTime}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<EventVO> getEventsInRange(
            @PathVariable long startTime,
            @PathVariable long endTime,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone,
            @RequestParam(required = false) String relationshipType) {

        List<EventVO> events;

        TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
        TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));

        try (Transaction tx = database.beginTx()) {
            events = convertEvents(timedEvents.getEvents(startTimeInstant, endTimeInstant, getRelationshipType(relationshipType)));
            tx.success();
        }

        return events;
    }

    @RequestMapping(value = "/{rootNodeId}/single/{time}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<EventVO> getEventsAtInstantWithCustomRoot(
            @PathVariable long rootNodeId,
            @PathVariable long time,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone,
            @RequestParam(required = false) String relationshipType) {

        List<EventVO> events;

        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));

        try (Transaction tx = database.beginTx()) {
            CustomRootTimeTree timeTree = new CustomRootTimeTree(database.getNodeById(rootNodeId));
            events = convertEvents(new TimeTreeTimedEvents(timeTree).getEvents(timeInstant, getRelationshipType(relationshipType)));
            tx.success();
        }

        return events;
    }

    @RequestMapping(value = "/{rootNodeId}/range/{startTime}/{endTime}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<EventVO> getEventsInRangeWithCustomRoot(
            @PathVariable long rootNodeId,
            @PathVariable long startTime,
            @PathVariable long endTime,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone,
            @RequestParam(required = false) String relationshipType) {

        List<EventVO> events;

        TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
        TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));

        try (Transaction tx = database.beginTx()) {
            CustomRootTimeTree timeTree = new CustomRootTimeTree(database.getNodeById(rootNodeId));
            events = convertEvents(new TimeTreeTimedEvents(timeTree).getEvents(startTimeInstant, endTimeInstant, getRelationshipType(relationshipType)));
            tx.success();
        }

        return events;
    }


    @RequestMapping(value = "{rootNodeId}/single/event", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.CREATED)
    public void attachEventToInstantWithCustomRoot(@RequestBody EventVO event, @PathVariable long rootNodeId) {
        event.validate();

        try (Transaction tx = database.beginTx()) {
            Node eventNode = database.getNodeById(event.getNodeId());
            CustomRootTimeTree timeTree = new CustomRootTimeTree(database.getNodeById(rootNodeId));

            new TimeTreeTimedEvents(timeTree).attachEvent(
                    eventNode,
                    DynamicRelationshipType.withName(event.getRelationshipType()),
                    TimeInstant.fromValueObject(event.getTimeInstant()));

            tx.success();
        }

    }

    private RelationshipType getRelationshipType(String relationshipType) {
        RelationshipType type = null;

        if (relationshipType != null) {
            type = DynamicRelationshipType.withName(relationshipType);
        }

        return type;
    }

    private List<EventVO> convertEvents(List<Event> events) {
        List<EventVO> eventVOs = new ArrayList<>(events.size());
        for (Event event : events) {
            eventVOs.add(event.toValueObject());
        }
        return eventVOs;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleIllegalArgument(IllegalArgumentException e) {
        return Collections.singletonMap("message", e.getMessage());
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> handleNotFound(NotFoundException e) {
        timeTree.invalidateCaches();
        return Collections.singletonMap("message", e.getMessage());
    }
}
