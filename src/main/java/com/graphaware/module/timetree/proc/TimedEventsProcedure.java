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
import com.graphaware.module.timetree.api.TimedEventVO;
import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.logic.TimedEventsBusinessLogic;
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
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class TimedEventsProcedure {

    private final TimedEventsBusinessLogic timedEventsLogic;
    private static final String PARAMETER_NAME_TIME = "time";
    private static final String PARAMETER_NAME_RESOLUTION = "resolution";
    private static final String PARAMETER_NAME_TIMEZONE = "timezone";
    private static final String PARAMETER_NAME_STAR_TIME = "starTime";
    private static final String PARAMETER_NAME_END_TIME = "endTime";
    private static final String PARAMETER_NAME_ROOT = "root";
    private static final String PARAMETER_NAME_RELATIONSHIP_TYPE = "relationshipType";
    private static final String PARAMETER_NAME_NODE = "node";
    private static final String PARAMETER_NAME_DIRECTION = "direction";
    private static final String PARAMETER_NAME_RELATIONSHIP_TYPES = "relationshipTypes";
    private static final String PARAMATER_NAME_INPUT = "input";

    public TimedEventsProcedure(GraphDatabaseService database, TimedEvents timedEvents) {
        this.timedEventsLogic = new TimedEventsBusinessLogic(database, timedEvents);
    }

    public CallableProcedure.BasicProcedure getEvents() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("single"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMATER_NAME_INPUT, Neo4jTypes.NTMap)
                .out(PARAMETER_NAME_NODE, Neo4jTypes.NTNode)
                .out(PARAMETER_NAME_RELATIONSHIP_TYPE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_DIRECTION, Neo4jTypes.NTString)
                .build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                Map<String, Object> inputParams = (Map) input[0];
                List<Event> events = timedEventsLogic.getEvents((long) inputParams.get(PARAMETER_NAME_TIME), 
                        (String) inputParams.get(PARAMETER_NAME_RESOLUTION), 
                        (String) inputParams.get(PARAMETER_NAME_TIMEZONE), 
                        (List<String>) inputParams.get(PARAMETER_NAME_RELATIONSHIP_TYPES), 
                        (String) inputParams.get(PARAMETER_NAME_DIRECTION));
                List<Object[]> collector = getObjectArray(events);
                return Iterators.asRawIterator(collector.iterator());
            }
        };
    }
    
    public CallableProcedure.BasicProcedure getAttach() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("attach"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_NODE, Neo4jTypes.NTNode)
                .in(PARAMETER_NAME_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_RELATIONSHIP_TYPE, Neo4jTypes.NTRelationship)
                .in(PARAMETER_NAME_DIRECTION, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_NODE, Neo4jTypes.NTNode)
                .build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                Node eventNode = (Node) input[0];
                boolean attachEvent = timedEventsLogic.attachEvent(eventNode, (RelationshipType) input[4], (String) input[5], (long) input[1], (String) input[3], (String) input[2]);
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[]{eventNode}).iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getRangeEvents() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("range"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_STAR_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_END_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_RELATIONSHIP_TYPES, Neo4jTypes.NTList(Neo4jTypes.NTString))
                .in(PARAMETER_NAME_DIRECTION, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_NODE, Neo4jTypes.NTNode)
                .out(PARAMETER_NAME_RELATIONSHIP_TYPE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_DIRECTION, Neo4jTypes.NTString)
                .build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                List<Event> events = timedEventsLogic.getEvents((long) input[0], (long) input[1], (String) input[2], (String) input[3], (List<String>) input[4], (String) input[5]);
                List<Object[]> collector = getObjectArray(events);
                return Iterators.asRawIterator(collector.iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getEventsWithCustomRoot() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("singleWithCustomRoot"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_ROOT, Neo4jTypes.NTNode)
                .in(PARAMETER_NAME_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_RELATIONSHIP_TYPES, Neo4jTypes.NTList(Neo4jTypes.NTString))
                .in(PARAMETER_NAME_DIRECTION, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_NODE, Neo4jTypes.NTNode)
                .out(PARAMETER_NAME_RELATIONSHIP_TYPE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_DIRECTION, Neo4jTypes.NTString)
                .build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                List<Event> events = timedEventsLogic.getEventsCustomRoot(((Node) input[0]).getId(), (long) input[1], (String) input[2], (String) input[3], (List<String>) input[4], (String) input[5]);
                List<Object[]> collector = getObjectArray(events);
                return Iterators.asRawIterator(collector.iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getRangeEventsWithCustomRoot() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("rangeWithCustomRoot"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_ROOT, Neo4jTypes.NTNode)
                .in(PARAMETER_NAME_STAR_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_END_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_RELATIONSHIP_TYPES, Neo4jTypes.NTList(Neo4jTypes.NTString))
                .in(PARAMETER_NAME_DIRECTION, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_NODE, Neo4jTypes.NTNode)
                .out(PARAMETER_NAME_RELATIONSHIP_TYPE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_DIRECTION, Neo4jTypes.NTString)
                .build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                List<Event> events = timedEventsLogic.getEventsCustomRoot(((Node) input[0]).getId(), (long) input[1], (long) input[2], (String) input[3], (String) input[4], (List<String>) input[5], (String) input[6]);
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

    private static ProcedureSignature.ProcedureName getProcedureName(String... procedureName) {
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
