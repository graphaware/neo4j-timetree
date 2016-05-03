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

import com.graphaware.module.timetree.logic.TimeTreeBusinessLogic;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class TimeTreeProcedure {

    private final TimeTreeBusinessLogic timeTree;

    public TimeTreeProcedure(GraphDatabaseService database) {
        timeTree = new TimeTreeBusinessLogic(database);

    }

    public CallableProcedure.BasicProcedure getOrCreateInstant() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("mergeInstant"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in("time", Neo4jTypes.NTNumber)
                .in("resolution", Neo4jTypes.NTString)
                .in("timezone", Neo4jTypes.NTString)
                .out("instant", Neo4jTypes.NTNode).build()) {            
            
            @Override
            public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input) throws ProcedureException {
                Node instant = timeTree.getOrCreateInstant((long)input[0], (String)input[1], (String)input[2]);
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[] {instant}).iterator());
            }
        };        
    }
    
    public CallableProcedure.BasicProcedure getInstant() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("single"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in("time", Neo4jTypes.NTNumber)
                .in("resolution", Neo4jTypes.NTString)
                .in("timezone", Neo4jTypes.NTString)
                .out("instant", Neo4jTypes.NTNode).build()) {            
            
            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                Node instant;
                try {
                    instant = timeTree.getInstant((long)input[0], (String)input[1], (String)input[2]);
                } 
                catch (NotFoundException ex) {
                    return Iterators.asRawIterator(Collections.<Object[]>emptyIterator());
                }
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[] {instant}).iterator());
            }
        };        
    }
    
    public CallableProcedure.BasicProcedure getInstants() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("range"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in("starTime", Neo4jTypes.NTNumber)
                .in("enTime", Neo4jTypes.NTNumber)
                .in("resolution", Neo4jTypes.NTString)
                .in("timezone", Neo4jTypes.NTString)
                .out("instants", Neo4jTypes.NTNode).build()) {            
            
            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                List<Node> instants = timeTree.getInstants((long)input[0], (long)input[1], (String)input[2], (String)input[3]);
                Set<Object[]> result = new HashSet<>();
                instants.stream().forEach((node) -> result.add(new Object[] {node}));
                return Iterators.asRawIterator(result.iterator());
            }
        };        
    }
    
    public CallableProcedure.BasicProcedure getOrCreateInstants() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("mergeInstants"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in("starTime", Neo4jTypes.NTNumber)
                .in("enTime", Neo4jTypes.NTNumber)
                .in("resolution", Neo4jTypes.NTString)
                .in("timezone", Neo4jTypes.NTString)
                .out("instants", Neo4jTypes.NTNode).build()) {            
            
            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                List<Node> instants = timeTree.getOrCreateInstants((long)input[0], (long)input[1], (String)input[2], (String)input[3]);
                Set<Object[]> result = new HashSet<>();
                instants.stream().forEach((node) -> result.add(new Object[] {node}));
                return Iterators.asRawIterator(result.iterator());
            }
        };        
    }

    private static ProcedureSignature.ProcedureName getProcedureName(String procedureName) {
        return procedureName("ga", "timetree", procedureName);
    }

}
