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
import org.joda.time.DateTimeZone;
import org.neo4j.graphdb.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.*;

import static com.graphaware.common.util.PropertyContainerUtils.ids;

/**
 * REST API for {@link com.graphaware.module.timetree.TimeTree}.
 */
@Controller
@RequestMapping("/timetree")
public class TimeTreeApi {

    private final GraphDatabaseService database;
    private final TimeTree timeTree;

    @Autowired
    public TimeTreeApi(GraphDatabaseService database) {
        this.database = database;
        timeTree = new SingleTimeTree(database);
    }

    @RequestMapping(value = "/single/{time}", method = RequestMethod.GET)
    @ResponseBody
    public long getInstant(
            @PathVariable(value = "time") long timeParam,
            @RequestParam(value = "resolution", required = false) String resolutionParam,
            @RequestParam(value = "timezone", required = false) String timeZoneParam) {

        long id;
        TimeInstant timeInstant = new TimeInstant(timeParam);
        if (resolutionParam != null) {
            timeInstant.setResolution(resolveResolution(resolutionParam));
        }
        if (timeZoneParam != null) {
            timeInstant.setTimezone(resolveTimeZone(timeZoneParam));
        }
        try (Transaction tx = database.beginTx()) {
            id = timeTree.getInstant(timeInstant, tx).getId();
            tx.success();
        }

        return id;
    }

    @RequestMapping(value = "/single/{time}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<EventTimeInstant> getEventsAtInstant(
            @PathVariable(value = "time") long timeParam,
            @RequestParam(value = "resolution", required = false) String resolutionParam,
            @RequestParam(value = "timezone", required = false) String timeZoneParam,
            @RequestParam(value = "eventRelation", required = false) String eventRelation) {

        List<EventTimeInstant> events;
        TimeInstant timeInstant = new TimeInstant(timeParam);
        RelationshipType eventRelationshipType = null;
        if (resolutionParam != null) {
            timeInstant.setResolution(resolveResolution(resolutionParam));
        }
        if (timeZoneParam != null) {
            timeInstant.setTimezone(resolveTimeZone(timeZoneParam));
        }
        if (eventRelation != null) {
            eventRelationshipType = DynamicRelationshipType.withName(eventRelation);
        }
        try (Transaction tx = database.beginTx()) {
            events = convertEvents(timeTree.getEventsAtInstant(timeInstant, eventRelationshipType, tx));
            tx.success();
        }

        return events;
    }

    @RequestMapping(value = "/single/event", method = RequestMethod.POST)
    @ResponseBody
    public void attachEventToInstant(@RequestBody EventTimeInstant event) {

        if (event.getTime() == 0 || event.getEventRelationshipType() == null || event.getEventRelationshipDirection() == null) {
            throw new IllegalArgumentException("Missing value for event time, event relationship type or event relationship direction");
        }


        TimeInstant timeInstant = new TimeInstant(event.getTime());

        try (Transaction tx = database.beginTx()) {
            Node eventNode = database.getNodeById(event.getEventNodeId());
            timeTree.attachEventToInstant(eventNode, resolveRelationship(event.getEventRelationshipType()), resolveDirection(event.getEventRelationshipDirection()), timeInstant, tx);
            tx.success();
        }

    }

    @RequestMapping(value = "/range/{startTime}/{endTime}", method = RequestMethod.GET)
    @ResponseBody
    public Long[] getInstants(
            @PathVariable(value = "startTime") long startTime,
            @PathVariable(value = "endTime") long endTime,
            @RequestParam(value = "resolution", required = false) String resolutionParam,
            @RequestParam(value = "timezone", required = false) String timeZoneParam) {

        Long[] ids;

        TimeInstant startTimeInstant = new TimeInstant(startTime);
        TimeInstant endTimeInstant = new TimeInstant(endTime);
        if (resolutionParam != null) {
            startTimeInstant.setResolution(resolveResolution(resolutionParam));
            endTimeInstant.setResolution(resolveResolution(resolutionParam));
        }
        if (timeZoneParam != null) {
            startTimeInstant.setTimezone(resolveTimeZone(timeZoneParam));
            endTimeInstant.setTimezone(resolveTimeZone(timeZoneParam));
        }
        try (Transaction tx = database.beginTx()) {
            ids = ids(timeTree.getInstants(startTimeInstant, endTimeInstant, tx));
            tx.success();
        }

        return ids;
    }

    @RequestMapping(value = "/range/{startTime}/{endTime}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<Event> getEventsInRange(
            @PathVariable(value = "startTime") long startTime,
            @PathVariable(value = "endTime") long endTime,
            @RequestParam(value = "resolution", required = false) String resolutionParam,
            @RequestParam(value = "timezone", required = false) String timeZoneParam,
            @RequestParam(value = "eventRelation", required = false) String eventRelation) {

        RelationshipType eventRelationshipType = null;
        List<Event> events;

        TimeInstant startTimeInstant = new TimeInstant(startTime);
        TimeInstant endTimeInstant = new TimeInstant(endTime);
        if (resolutionParam != null) {
            startTimeInstant.setResolution(resolveResolution(resolutionParam));
            endTimeInstant.setResolution(resolveResolution(resolutionParam));
        }
        if (timeZoneParam != null) {
            startTimeInstant.setTimezone(resolveTimeZone(timeZoneParam));
            endTimeInstant.setTimezone(resolveTimeZone(timeZoneParam));
        }
        if (eventRelation != null) {
            eventRelationshipType = DynamicRelationshipType.withName(eventRelation);
        }
        try (Transaction tx = database.beginTx()) {
            events = timeTree.getEventsBetweenInstants(startTimeInstant, endTimeInstant, eventRelationshipType, tx);
            tx.success();
        }

        return events;
    }

    @RequestMapping(value = "/{rootNodeId}/single/{time}", method = RequestMethod.GET)
    @ResponseBody
    public long getInstantWithCustomRoot(
            @PathVariable(value = "rootNodeId") long rootNodeId,
            @PathVariable(value = "time") long timeParam,
            @RequestParam(value = "resolution", required = false) String resolutionParam,
            @RequestParam(value = "timezone", required = false) String timeZoneParam) {

        long id;
        TimeInstant timeInstant = new TimeInstant(timeParam);
        if (resolutionParam != null) {
            timeInstant.setResolution(resolveResolution(resolutionParam));
        }
        if (timeZoneParam != null) {
            timeInstant.setTimezone(resolveTimeZone(timeZoneParam));
        }
        try (Transaction tx = database.beginTx()) {
            id = new CustomRootTimeTree(database.getNodeById(rootNodeId)).getInstant(timeInstant, tx).getId();
            tx.success();
        }

        return id;
    }

    @RequestMapping(value = "/{rootNodeId}/range/{startTime}/{endTime}", method = RequestMethod.GET)
    @ResponseBody
    public Long[] getInstantsWithCustomRoot(
            @PathVariable(value = "rootNodeId") long rootNodeId,
            @PathVariable(value = "startTime") long startTime,
            @PathVariable(value = "endTime") long endTime,
            @RequestParam(value = "resolution", required = false) String resolutionParam,
            @RequestParam(value = "timezone", required = false) String timeZoneParam) {

        Long[] ids;
        TimeInstant startTimeInstant = new TimeInstant(startTime);
        TimeInstant endTimeInstant = new TimeInstant(endTime);
        if (resolutionParam != null) {
            startTimeInstant.setResolution(resolveResolution(resolutionParam));
            endTimeInstant.setResolution(resolveResolution(resolutionParam));
        }
        if (timeZoneParam != null) {
            startTimeInstant.setTimezone(resolveTimeZone(timeZoneParam));
            endTimeInstant.setTimezone(resolveTimeZone(timeZoneParam));
        }
        try (Transaction tx = database.beginTx()) {
            ids = ids(new CustomRootTimeTree(database.getNodeById(rootNodeId)).getInstants(startTimeInstant, endTimeInstant, tx));
            tx.success();
        }

        return ids;
    }

    @RequestMapping(value = "/{rootNodeId}/single/{time}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<Event> getEventsAtInstantWithCustomRoot(
            @PathVariable(value = "rootNodeId") long rootNodeId,
            @PathVariable(value = "time") long timeParam,
            @RequestParam(value = "resolution", required = false) String resolutionParam,
            @RequestParam(value = "timezone", required = false) String timeZoneParam,
            @RequestParam(value = "eventRelation", required = false) String eventRelation) {

        List<Event> events;
        TimeInstant timeInstant = new TimeInstant(timeParam);
        RelationshipType eventRelationshipType = null;

        if (resolutionParam != null) {
            timeInstant.setResolution(resolveResolution(resolutionParam));
        }
        if (timeZoneParam != null) {
            timeInstant.setTimezone(resolveTimeZone(timeZoneParam));
        }
        if (eventRelation != null) {
            eventRelationshipType = DynamicRelationshipType.withName(eventRelation);
        }
        try (Transaction tx = database.beginTx()) {
            events = new CustomRootTimeTree(database.getNodeById(rootNodeId)).getEventsAtInstant(timeInstant, eventRelationshipType, tx);
            tx.success();
        }

        return events;
    }

    @RequestMapping(value = "/{rootNodeId}/range/{startTime}/{endTime}/events", method = RequestMethod.GET)
    @ResponseBody
    public List<Event> getEventsInRangeWithCustomRoot(
            @PathVariable(value = "rootNodeId") long rootNodeId,
            @PathVariable(value = "startTime") long startTime,
            @PathVariable(value = "endTime") long endTime,
            @RequestParam(value = "resolution", required = false) String resolutionParam,
            @RequestParam(value = "timezone", required = false) String timeZoneParam,
            @RequestParam(value = "eventRelation", required = false) String eventRelation) {

        List<Event> events;
        RelationshipType eventRelationshipType = null;
        TimeInstant startTimeInstant = new TimeInstant(startTime);
        TimeInstant endTimeInstant = new TimeInstant(endTime);
        if (resolutionParam != null) {
            startTimeInstant.setResolution(resolveResolution(resolutionParam));
            endTimeInstant.setResolution(resolveResolution(resolutionParam));
        }
        if (timeZoneParam != null) {
            startTimeInstant.setTimezone(resolveTimeZone(timeZoneParam));
            endTimeInstant.setTimezone(resolveTimeZone(timeZoneParam));
        }
        if (eventRelation != null) {
            eventRelationshipType = DynamicRelationshipType.withName(eventRelation);
        }
        try (Transaction tx = database.beginTx()) {
            events = new CustomRootTimeTree(database.getNodeById(rootNodeId)).getEventsBetweenInstants(startTimeInstant, endTimeInstant, eventRelationshipType, tx);
            tx.success();
        }

        return events;
    }

    @RequestMapping(value = "{rootNodeId}/single/event", method = RequestMethod.POST)
    @ResponseBody
    public void attachEventToInstantWithCustomRoot(@RequestBody EventTimeInstant event, @PathVariable(value = "rootNodeId") long rootNodeId) {

        if (event.getTime() == 0 || event.getEventRelationshipType() == null || event.getEventRelationshipDirection() == null) {
            throw new IllegalArgumentException("Missing value for event time, event relationship type or event relationship direction");
        }


        TimeInstant timeInstant = new TimeInstant(event.getTime());

        try (Transaction tx = database.beginTx()) {
            Node eventNode = database.getNodeById(event.getEventNodeId());
            new CustomRootTimeTree(database.getNodeById(rootNodeId)).attachEventToInstant(eventNode, resolveRelationship(event.getEventRelationshipType()), resolveDirection(event.getEventRelationshipDirection()), timeInstant, tx);
            tx.success();
        }

    }


    @RequestMapping(value = "/now", method = RequestMethod.GET)
    @ResponseBody
    public long getNow(
            @RequestParam(value = "resolution", required = false) String resolutionParam,
            @RequestParam(value = "timezone", required = false) String timeZoneParam) {

        long id;
        TimeInstant timeInstant = new TimeInstant();
        if (timeZoneParam != null) {
            timeInstant.setTimezone(resolveTimeZone(timeZoneParam));
        }
        if (resolutionParam != null) {
            timeInstant.setResolution(resolveResolution(resolutionParam));
        }
        try (Transaction tx = database.beginTx()) {
            id = timeTree.getNow(timeInstant, tx).getId();
            tx.success();
        }

        return id;
    }

    @RequestMapping(value = "/{rootNodeId}/now", method = RequestMethod.GET)
    @ResponseBody
    public long getNowWithCustomRoot(
            @PathVariable(value = "rootNodeId") long rootNodeId,
            @RequestParam(value = "resolution", required = false) String resolutionParam,
            @RequestParam(value = "timezone", required = false) String timeZoneParam) {

        long id;
        TimeInstant timeInstant = new TimeInstant();
        if (timeZoneParam != null) {
            timeInstant.setTimezone(resolveTimeZone(timeZoneParam));
        }
        if (resolutionParam != null) {
            timeInstant.setResolution(resolveResolution(resolutionParam));
        }
        try (Transaction tx = database.beginTx()) {
            id = new CustomRootTimeTree(database.getNodeById(rootNodeId)).getNow(timeInstant, tx).getId();
            tx.success();
        }

        return id;
    }

    private DateTimeZone resolveTimeZone(String timeZoneParam) {
        DateTimeZone timeZone = null;
        if (timeZoneParam != null) {
            timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZoneParam));
        }
        return timeZone;
    }

    private Resolution resolveResolution(String resolutionParam) {
        Resolution resolution = null;
        if (resolutionParam != null) {
            resolution = Resolution.valueOf(resolutionParam.toUpperCase());
        }
        return resolution;
    }

    private RelationshipType resolveRelationship(String relationship) {
        RelationshipType relationshipType = null;
        if (relationship != null) {
            relationshipType = DynamicRelationshipType.withName(relationship);
        }
        return relationshipType;
    }

    private Direction resolveDirection(String direction) {
        Direction dir = null;
        if (direction != null) {
            dir = Direction.valueOf(direction);
        }
        return dir;
    }

    private List<EventTimeInstant> convertEvents(List<Event> events) {
        List<EventTimeInstant> eventTimeInstants = new ArrayList<>(events.size());
        for (Event event : events) {
            eventTimeInstants.add(new EventTimeInstant(event));
        }
        return eventTimeInstants;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleIllegalArgument(IllegalArgumentException e) {
        return Collections.singletonMap("message", e.getMessage());
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> handleNotFound(NotFoundException e) {
        return Collections.singletonMap("message", e.getMessage());
    }
}
