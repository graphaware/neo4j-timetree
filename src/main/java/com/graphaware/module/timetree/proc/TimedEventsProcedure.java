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
import java.util.List;
import java.util.stream.Collectors;
import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class TimedEventsProcedure {

    private final TimedEventsBusinessLogic timedEventsLogic;

    public TimedEventsProcedure(GraphDatabaseService database, TimedEvents timedEvents) {
        this.timedEventsLogic = new TimedEventsBusinessLogic(database, timedEvents);
    }
    
    public CallableProcedure.BasicProcedure getEvents() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("single"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in("time", Neo4jTypes.NTNumber)
                .in("resolution", Neo4jTypes.NTString)
                .in("timezone", Neo4jTypes.NTString)
                .in("relationshipTypes", Neo4jTypes.NTList(Neo4jTypes.NTString))
                .in("direction", Neo4jTypes.NTString)
                .out("node", Neo4jTypes.NTNode)
                .out("relationshipType", Neo4jTypes.NTRelationship)
                .out("direction", Neo4jTypes.NTString)
                .build()) {            
            
            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                List<Event> events = timedEventsLogic.getEvents((long)input[0], (String)input[1], (String)input[2], (List<String>)input[3], (String)input[4]);
                List<Object[]> collector = events.stream()
                        .map((event) -> new Object[] {event.getNode(), event.getRelationshipType(), event.getDirection().name()})
                        .collect(Collectors.toList());
                return Iterators.asRawIterator(collector.iterator());
            }
        };        
    }

    private static ProcedureSignature.ProcedureName getProcedureName(String procedureName) {
        return procedureName("ga", "timedevents", procedureName);
    }

}
