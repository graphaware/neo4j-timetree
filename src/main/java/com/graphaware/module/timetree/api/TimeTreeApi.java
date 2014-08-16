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
import com.graphaware.module.timetree.domain.TimeInstant;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.Map;

import static com.graphaware.common.util.PropertyContainerUtils.ids;

/**
 * REST API for {@link com.graphaware.module.timetree.TimeTree}.
 */
@Controller
@RequestMapping("/timetree")
public class TimeTreeApi {
    private static final Logger LOG = LoggerFactory.getLogger(TimeTreeApi.class);

    private final GraphDatabaseService database;
    private final TimeTree timeTree;

    @Autowired
    public TimeTreeApi(GraphDatabaseService database) {
        this.database = database;
        this.timeTree = new SingleTimeTree(database);
    }

    @RequestMapping(value = "/single/{time}", method = RequestMethod.GET)
    @ResponseBody
    public long getInstant(
            @PathVariable long time,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        long id;

        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));

        try (Transaction tx = database.beginTx()) {
            id = timeTree.getOrCreateInstant(timeInstant).getId();
            tx.success();
        }

        return id;
    }

    @RequestMapping(value = "/range/{startTime}/{endTime}", method = RequestMethod.GET)
    @ResponseBody
    public Long[] getInstants(
            @PathVariable long startTime,
            @PathVariable long endTime,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        Long[] ids;

        TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
        TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));

        try (Transaction tx = database.beginTx()) {
            ids = ids(timeTree.getOrCreateInstants(startTimeInstant, endTimeInstant));
            tx.success();
        }

        return ids;
    }

    @RequestMapping(value = "/{rootNodeId}/single/{time}", method = RequestMethod.GET)
    @ResponseBody
    public long getInstantWithCustomRoot(
            @PathVariable long rootNodeId,
            @PathVariable long time,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        long id;

        TimeInstant timeInstant = TimeInstant.fromValueObject(new TimeInstantVO(time, resolution, timezone));

        try (Transaction tx = database.beginTx()) {
            id = new CustomRootTimeTree(database.getNodeById(rootNodeId)).getOrCreateInstant(timeInstant).getId();
            tx.success();
        }

        return id;
    }


    @RequestMapping(value = "/{rootNodeId}/range/{startTime}/{endTime}", method = RequestMethod.GET)
    @ResponseBody
    public Long[] getInstantsWithCustomRoot(
            @PathVariable long rootNodeId,
            @PathVariable long startTime,
            @PathVariable long endTime,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        Long[] ids;

        TimeInstant startTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(startTime, resolution, timezone));
        TimeInstant endTimeInstant = TimeInstant.fromValueObject(new TimeInstantVO(endTime, resolution, timezone));

        try (Transaction tx = database.beginTx()) {
            ids = ids(new CustomRootTimeTree(database.getNodeById(rootNodeId)).getOrCreateInstants(startTimeInstant, endTimeInstant));
            tx.success();
        }

        return ids;
    }

    @RequestMapping(value = "/now", method = RequestMethod.GET)
    @ResponseBody
    public long getNow(
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        return getInstant(System.currentTimeMillis(), resolution, timezone);
    }


    @RequestMapping(value = "/{rootNodeId}/now", method = RequestMethod.GET)
    @ResponseBody
    public long getNowWithCustomRoot(
            @PathVariable long rootNodeId,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        return getInstantWithCustomRoot(rootNodeId, System.currentTimeMillis(), resolution, timezone);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    public Map<String, String> handleIllegalArgument(IllegalArgumentException e) {
        LOG.warn("Bad Request: "+e.getMessage(), e);
        return Collections.singletonMap("message", e.getMessage());
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ResponseBody
    public Map<String, String> handleNotFound(NotFoundException e) {
        LOG.warn("Not Found: "+e.getMessage(), e);
        return Collections.singletonMap("message", e.getMessage());
    }
}
