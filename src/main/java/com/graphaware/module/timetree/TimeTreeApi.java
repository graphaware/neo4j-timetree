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

package com.graphaware.module.timetree;

import org.joda.time.DateTimeZone;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.TimeZone;

import static com.graphaware.common.util.PropertyContainerUtils.ids;

/**
 * REST API for {@link TimeTree}.
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
            ids = ids(timeTree.getInstants(startTimeInstant,endTimeInstant, tx));
            tx.success();
        }

        return ids;
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
            ids = ids(new CustomRootTimeTree(database.getNodeById(rootNodeId)).getInstants(startTimeInstant,endTimeInstant, tx));
            tx.success();
        }

        return ids;
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

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public void handleIllegalArguments() {
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public void handleNotFound() {
    }
}
