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
package com.graphaware.module.timetree.api;

import com.graphaware.api.json.JsonNode;
import com.graphaware.api.json.LongIdJsonNode;
import com.graphaware.module.timetree.SingleTimeTree;
import com.graphaware.module.timetree.TimeTree;
import com.graphaware.module.timetree.logic.TimeTreeBusinessLogic;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * REST API for {@link com.graphaware.module.timetree.TimeTree}.
 */
@Controller
@RequestMapping("/timetree")
public class TimeTreeApi {

    private static final Logger LOG = LoggerFactory.getLogger(TimeTreeApi.class);

    private final GraphDatabaseService database;
    
    private final TimeTreeBusinessLogic timeTreeLogic;

    @Autowired
    public TimeTreeApi(GraphDatabaseService database) {
        this.database = database;
        this.timeTreeLogic = new TimeTreeBusinessLogic(database);
    }
    
    private JsonNode getJsonNode(Node instant) {
        JsonNode result;
        try (Transaction tx = database.beginTx()) {
            result = new LongIdJsonNode(instant);
            tx.success();
        }
        return result;
    }

    private JsonNode[] getJsonNodes(List<Node> nodes) {
        JsonNode[] result;
        try (Transaction tx = database.beginTx()) {
            result = jsonNodes(nodes);
            tx.success();
        }
        return result;
    }

    @RequestMapping(value = "/single/{time}", method = RequestMethod.GET)
    @ResponseBody
    public JsonNode getInstant(
            @PathVariable long time,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        Node instant = timeTreeLogic.getInstant(time, resolution, timezone);
        JsonNode result = getJsonNode(instant);
        return result;
    }

    @RequestMapping(value = "/single/{time}", method = RequestMethod.POST)
    @ResponseBody
    public JsonNode getOrCreateInstant(
            @PathVariable long time,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        Node instant = timeTreeLogic.getOrCreateInstant(time, resolution, timezone);
        JsonNode result = getJsonNode(instant);
        return result;
    }

    @RequestMapping(value = "/range/{startTime}/{endTime}", method = RequestMethod.GET)
    @ResponseBody
    public JsonNode[] getInstants(
            @PathVariable long startTime,
            @PathVariable long endTime,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        List<Node> nodes = timeTreeLogic.getInstants(startTime, endTime, resolution, timezone);
        JsonNode[] result = getJsonNodes(nodes);
        return result;
    }

    @RequestMapping(value = "/range/{startTime}/{endTime}", method = RequestMethod.POST)
    @ResponseBody
    public JsonNode[] getOrCreateInstants(
            @PathVariable long startTime,
            @PathVariable long endTime,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        List<Node> nodes = timeTreeLogic.getOrCreateInstants(startTime, resolution, timezone, endTime);
        JsonNode[] result = getJsonNodes(nodes);
        return result;
    }

    @RequestMapping(value = "/{rootNodeId}/single/{time}", method = RequestMethod.GET)
    @ResponseBody
    public JsonNode getInstantWithCustomRoot(
            @PathVariable long rootNodeId,
            @PathVariable long time,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        Node instant = timeTreeLogic.getInstantWithCustomRoot(time, resolution, timezone, rootNodeId);
        JsonNode result = getJsonNode(instant);
        return result;
    }

    @RequestMapping(value = "/{rootNodeId}/single/{time}", method = RequestMethod.POST)
    @ResponseBody
    public JsonNode getOrCreateInstantWithCustomRoot(
            @PathVariable long rootNodeId,
            @PathVariable long time,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        Node instant = timeTreeLogic.getOrCreateInstantWithCustomRoot(time, resolution, timezone, rootNodeId);
        JsonNode result = getJsonNode(instant);
        return result;
    }


    @RequestMapping(value = "/{rootNodeId}/range/{startTime}/{endTime}", method = RequestMethod.GET)
    @ResponseBody
    public JsonNode[] getInstantsWithCustomRoot(
            @PathVariable long rootNodeId,
            @PathVariable long startTime,
            @PathVariable long endTime,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        List<Node> nodes = timeTreeLogic.getInstantsWithCustomRoot(startTime, resolution, timezone, endTime, rootNodeId);
        JsonNode[] result = getJsonNodes(nodes);
        return result;
    }

    @RequestMapping(value = "/{rootNodeId}/range/{startTime}/{endTime}", method = RequestMethod.POST)
    @ResponseBody
    public JsonNode[] getOrCreateInstantsWithCustomRoot(
            @PathVariable long rootNodeId,
            @PathVariable long startTime,
            @PathVariable long endTime,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        List<Node> nodes = timeTreeLogic.getOrCreateInstantsWithCustomRoot(startTime, resolution, timezone, endTime, rootNodeId);
        JsonNode[] result = getJsonNodes(nodes);
        return result;
    }

    @RequestMapping(value = "/now", method = RequestMethod.GET)
    @ResponseBody
    public JsonNode getNow(
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        return getInstant(System.currentTimeMillis(), resolution, timezone);
    }

    @RequestMapping(value = "/now", method = RequestMethod.POST)
    @ResponseBody
    public JsonNode getOrCreateNow(
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        return getOrCreateInstant(System.currentTimeMillis(), resolution, timezone);
    }

    @RequestMapping(value = "/{rootNodeId}/now", method = RequestMethod.GET)
    @ResponseBody
    public JsonNode getNowWithCustomRoot(
            @PathVariable long rootNodeId,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        return getInstantWithCustomRoot(rootNodeId, System.currentTimeMillis(), resolution, timezone);
    }

    @RequestMapping(value = "/{rootNodeId}/now", method = RequestMethod.POST)
    @ResponseBody
    public JsonNode getOrCreateNowWithCustomRoot(
            @PathVariable long rootNodeId,
            @RequestParam(required = false) String resolution,
            @RequestParam(required = false) String timezone) {

        return getOrCreateInstantWithCustomRoot(rootNodeId, System.currentTimeMillis(), resolution, timezone);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    public Map<String, String> handleIllegalArgument(IllegalArgumentException e) {
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

    private JsonNode[] jsonNodes(List<Node> nodes) {
        List<JsonNode> result = new LinkedList<>();
        for (Node node : nodes) {
            result.add(new LongIdJsonNode(node));
        }
        return result.toArray(new JsonNode[result.size()]);
    }
}
