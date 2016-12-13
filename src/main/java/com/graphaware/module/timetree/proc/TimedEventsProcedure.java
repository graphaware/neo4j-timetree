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
package com.graphaware.module.timetree.proc;

import com.graphaware.module.timetree.TimedEvents;
import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.logic.TimedEventsBusinessLogic;
import static com.graphaware.module.timetree.proc.TimeTreeBaseProcedure.PARAMETER_NAME_END_TIME;
import static com.graphaware.module.timetree.proc.TimeTreeBaseProcedure.PARAMETER_NAME_ROOT;
import static com.graphaware.module.timetree.proc.TimeTreeBaseProcedure.PARAMETER_NAME_START_TIME;
import static com.graphaware.module.timetree.proc.TimeTreeBaseProcedure.PARAMETER_NAME_TIME;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.*;

import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class TimedEventsProcedure extends TimeTreeBaseProcedure {

    private final TimedEventsBusinessLogic timedEventsLogic;

    public TimedEventsProcedure(GraphDatabaseService database, TimedEvents timedEvents) {
        this.timedEventsLogic = new TimedEventsBusinessLogic(database, timedEvents);
    }

    public CallableProcedure.BasicProcedure getEvents() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("single"))
                .mode(Mode.READ_WRITE)
                .in(PARAMETER_NAME_INPUT, Neo4jTypes.NTMap)
                .out(PARAMETER_NAME_NODE, Neo4jTypes.NTNode)
                .out(PARAMETER_NAME_RELATIONSHIP_TYPE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_DIRECTION, Neo4jTypes.NTString)
                .build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input) throws ProcedureException {
                checkIsMap(input[0]);
                Map<String, Object> inputParams = (Map) input[0];
                checkTime(inputParams, PARAMETER_NAME_TIME);
                List<Event> events;
                if (inputParams.containsKey(PARAMETER_NAME_ROOT)) {
                    events = timedEventsLogic.getEventsCustomRoot(((Node) inputParams.get(PARAMETER_NAME_ROOT)).getId(),
                            (long) inputParams.get(PARAMETER_NAME_TIME),
                            (String) inputParams.get(PARAMETER_NAME_RESOLUTION),
                            (String) inputParams.get(PARAMETER_NAME_TIMEZONE),
                            (List<String>) inputParams.get(PARAMETER_NAME_RELATIONSHIP_TYPES),
                            (String) inputParams.get(PARAMETER_NAME_DIRECTION));
                } else {
                    events = timedEventsLogic.getEvents((long) inputParams.get(PARAMETER_NAME_TIME),
                            (String) inputParams.get(PARAMETER_NAME_RESOLUTION),
                            (String) inputParams.get(PARAMETER_NAME_TIMEZONE),
                            (List<String>) inputParams.get(PARAMETER_NAME_RELATIONSHIP_TYPES),
                            (String) inputParams.get(PARAMETER_NAME_DIRECTION));
                }
                List<Object[]> collector = getObjectArray(events);
                return Iterators.asRawIterator(collector.iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getAttach() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("attach"))
                .mode(Mode.READ_WRITE)
                .in(PARAMETER_NAME_INPUT, Neo4jTypes.NTMap)
                .out(PARAMETER_NAME_NODE, Neo4jTypes.NTNode)
                .build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input) throws ProcedureException {
                checkIsMap(input[0]);
                Map<String, Object> inputParams = (Map) input[0];
                checkTime(inputParams, PARAMETER_NAME_TIME);
                Node eventNode = (Node) inputParams.get(PARAMETER_NAME_NODE);
                checkEventNode(eventNode);
                if (inputParams.containsKey(PARAMETER_NAME_ROOT)) {
                    timedEventsLogic.attachEventWithCustomRoot((Node) inputParams.get(PARAMETER_NAME_ROOT),
                            eventNode,
                            getRelationshipType((String) inputParams.get(PARAMETER_NAME_RELATIONSHIP_TYPE)),
                            (String) inputParams.get(PARAMETER_NAME_DIRECTION),
                            (long) inputParams.get(PARAMETER_NAME_TIME),
                            (String) inputParams.get(PARAMETER_NAME_TIMEZONE),
                            (String) inputParams.get(PARAMETER_NAME_RESOLUTION));
                } else {
                    timedEventsLogic.attachEvent(eventNode,
                            getRelationshipType((String) inputParams.get(PARAMETER_NAME_RELATIONSHIP_TYPE)),
                            (String) inputParams.get(PARAMETER_NAME_DIRECTION),
                            (long) inputParams.get(PARAMETER_NAME_TIME),
                            (String) inputParams.get(PARAMETER_NAME_TIMEZONE),
                            (String) inputParams.get(PARAMETER_NAME_RESOLUTION));
                }
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[]{eventNode}).iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getRangeEvents() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("range"))
                .mode(Mode.READ_WRITE)
                .in(PARAMETER_NAME_INPUT, Neo4jTypes.NTMap)
                .out(PARAMETER_NAME_NODE, Neo4jTypes.NTNode)
                .out(PARAMETER_NAME_RELATIONSHIP_TYPE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_DIRECTION, Neo4jTypes.NTString)
                .build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input) throws ProcedureException {
                checkIsMap(input[0]);
                Map<String, Object> inputParams = (Map) input[0];
                checkTime(inputParams, PARAMETER_NAME_START_TIME);
                checkTime(inputParams, PARAMETER_NAME_END_TIME);
                List<Event> events;
                if (inputParams.containsKey(PARAMETER_NAME_ROOT)) {
                    events = timedEventsLogic.getEventsCustomRoot(
                            (long) ((Node) inputParams.get(PARAMETER_NAME_ROOT)).getId(),
                            (long) inputParams.get(PARAMETER_NAME_START_TIME),
                            (long) inputParams.get(PARAMETER_NAME_END_TIME),
                            (String) inputParams.get(PARAMETER_NAME_RESOLUTION),
                            (String) inputParams.get(PARAMETER_NAME_TIMEZONE),
                            (List<String>) inputParams.get(PARAMETER_NAME_RELATIONSHIP_TYPES),
                            (String) inputParams.get(PARAMETER_NAME_DIRECTION));
                } else {
                    events = timedEventsLogic.getEvents(
                            (long) inputParams.get(PARAMETER_NAME_START_TIME),
                            (long) inputParams.get(PARAMETER_NAME_END_TIME),
                            (String) inputParams.get(PARAMETER_NAME_RESOLUTION),
                            (String) inputParams.get(PARAMETER_NAME_TIMEZONE),
                            (List<String>) inputParams.get(PARAMETER_NAME_RELATIONSHIP_TYPES),
                            (String) inputParams.get(PARAMETER_NAME_DIRECTION));
                }
                List<Object[]> collector = getObjectArray(events);
                return Iterators.asRawIterator(collector.iterator());
            }
        };
    }

    private List<Object[]> getObjectArray(List<Event> events) {
        List<Object[]> collector = events.stream()
                .map((event) -> new Object[]{event.getNode(), event.getRelationshipType() != null ? event.getRelationshipType().toString() : "",
            event.getDirection().name()})
                .collect(Collectors.toList());
        return collector;
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

    protected static QualifiedName getProcedureName(String... procedureName) {
        String namespace[] = new String[3 + procedureName.length];
        int i = 0;
        namespace[i++] = "ga";
        namespace[i++] = "timetree";
        namespace[i++] = "events";

        for (String value : procedureName) {
            namespace[i++] = value;
        }
        return procedureName(namespace);
    }

}
