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
package com.graphaware.module.timetree.proc;

import com.graphaware.module.timetree.SingleTimeTree;
import com.graphaware.module.timetree.TimeTreeBackedEvents;
import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.logic.TimedEventsBusinessLogic;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TimedEventsProcedure extends TimeTreeBaseProcedure {

    @Context
    public GraphDatabaseAPI db;

    private TimedEventsBusinessLogic initTimeTree(GraphDatabaseService db) {
        return new TimedEventsBusinessLogic(db, new TimeTreeBackedEvents(new SingleTimeTree(db)));
    }

    @Procedure(mode = Mode.WRITE, name = "ga.timetree.events.single")
    @Description(value = "CALL ga.timetree.events.single({time: 1463659567468}) YIELD node RETURN node")
    public Stream<EventResult> single(@Name("params") Map<String, Object> params) {
        final TimedEventsBusinessLogic timedEventsLogic = initTimeTree(db);

        checkTime(params, PARAMETER_NAME_TIME);
        List<Event> events;
        if (params.containsKey(PARAMETER_NAME_ROOT)) {
            events = timedEventsLogic.getEventsCustomRoot(((Node) params.get(PARAMETER_NAME_ROOT)).getId(),
                    (long) params.get(PARAMETER_NAME_TIME),
                    (String) params.get(PARAMETER_NAME_RESOLUTION),
                    (String) params.get(PARAMETER_NAME_TIMEZONE),
                    (List<String>) params.get(PARAMETER_NAME_RELATIONSHIP_TYPES),
                    (String) params.get(PARAMETER_NAME_DIRECTION));
        } else {
            events = timedEventsLogic.getEvents((long) params.get(PARAMETER_NAME_TIME),
                    (String) params.get(PARAMETER_NAME_RESOLUTION),
                    (String) params.get(PARAMETER_NAME_TIMEZONE),
                    (List<String>) params.get(PARAMETER_NAME_RELATIONSHIP_TYPES),
                    (String) params.get(PARAMETER_NAME_DIRECTION));
        }

        return events.stream().map(event -> new EventResult(event));
    }

    @Procedure(mode = Mode.WRITE, name = "ga.timetree.events.attach")
    @Description(value = "CALL ga.timetree.events.attach({node: e, time: 1463659567468, relationshipType: \"SENT_ON\"}) YIELD node RETURN node")
    public Stream<NodeResult> attach(@Name("params") Map<String, Object> params) {
        final TimedEventsBusinessLogic timedEventsLogic = initTimeTree(db);

        checkTime(params, PARAMETER_NAME_TIME);
        Node eventNode = (Node) params.get(PARAMETER_NAME_NODE);
        checkEventNode(eventNode);
        if (params.containsKey(PARAMETER_NAME_ROOT)) {
            timedEventsLogic.attachEventWithCustomRoot((Node) params.get(PARAMETER_NAME_ROOT),
                    eventNode,
                    getRelationshipType((String) params.get(PARAMETER_NAME_RELATIONSHIP_TYPE)),
                    (String) params.get(PARAMETER_NAME_DIRECTION),
                    (long) params.get(PARAMETER_NAME_TIME),
                    (String) params.get(PARAMETER_NAME_TIMEZONE),
                    (String) params.get(PARAMETER_NAME_RESOLUTION));
        } else {
            timedEventsLogic.attachEvent(eventNode,
                    getRelationshipType((String) params.get(PARAMETER_NAME_RELATIONSHIP_TYPE)),
                    (String) params.get(PARAMETER_NAME_DIRECTION),
                    (long) params.get(PARAMETER_NAME_TIME),
                    (String) params.get(PARAMETER_NAME_TIMEZONE),
                    (String) params.get(PARAMETER_NAME_RESOLUTION));
        }

        return Stream.of(new NodeResult(eventNode));
    }

    @Procedure(mode = Mode.WRITE, name = "ga.timetree.events.range")
    @Description(value = "CALL ga.timetree.events.range({start: 1463659567468, end: 1463859569504}) YIELD node, relationshipType, direction RETURN *")
    public Stream<EventResult> range(@Name("params") Map<String, Object> params) {
        final TimedEventsBusinessLogic timedEventsLogic = initTimeTree(db);

        checkTime(params, PARAMETER_NAME_START_TIME);
        checkTime(params, PARAMETER_NAME_END_TIME);
        List<Event> events;
        if (params.containsKey(PARAMETER_NAME_ROOT)) {
            events = timedEventsLogic.getEventsCustomRoot(
                    ((Node) params.get(PARAMETER_NAME_ROOT)).getId(),
                    (long) params.get(PARAMETER_NAME_START_TIME),
                    (long) params.get(PARAMETER_NAME_END_TIME),
                    (String) params.get(PARAMETER_NAME_RESOLUTION),
                    (String) params.get(PARAMETER_NAME_TIMEZONE),
                    (List<String>) params.get(PARAMETER_NAME_RELATIONSHIP_TYPES),
                    (String) params.get(PARAMETER_NAME_DIRECTION));
        } else {
            events = timedEventsLogic.getEvents(
                    (long) params.get(PARAMETER_NAME_START_TIME),
                    (long) params.get(PARAMETER_NAME_END_TIME),
                    (String) params.get(PARAMETER_NAME_RESOLUTION),
                    (String) params.get(PARAMETER_NAME_TIMEZONE),
                    (List<String>) params.get(PARAMETER_NAME_RELATIONSHIP_TYPES),
                    (String) params.get(PARAMETER_NAME_DIRECTION));
        }

        return events.stream().map(event -> new EventResult(event));
    }

    private void checkEventNode(Node eventNode) {
        if (eventNode == null)
            throw new RuntimeException("Event node is necessary. Parameter " + PARAMETER_NAME_NODE + " is missing");
    }

    private RelationshipType getRelationshipType(String relType) {
        if (null == relType || relType.trim().equals("")) {
            throw new RuntimeException("The given relationship type cannot be null or an empty string");
        }

        return RelationshipType.withName(relType);
    }
}