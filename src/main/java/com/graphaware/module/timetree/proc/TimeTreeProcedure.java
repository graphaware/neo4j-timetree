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
import java.util.Map;
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

public class TimeTreeProcedure extends TimeTreeBaseProcedure {


    private final TimeTreeBusinessLogic timeTree;

    public TimeTreeProcedure(GraphDatabaseService database) {
        timeTree = new TimeTreeBusinessLogic(database);

    }

    public CallableProcedure.BasicProcedure get() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("single"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_INPUT, Neo4jTypes.NTMap)
                .out(PARAMETER_NAME_INSTANT, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input) throws ProcedureException {
                validateSingleParamter(input[0]);
                Map<String, Object> inputParams = (Map) input[0];
                boolean create = (boolean) inputParams.getOrDefault(PARAMETER_NAME_CREATE, false);
                Node rootNode = (Node) inputParams.getOrDefault(PARAMETER_NAME_ROOT, null);
                long time = (long) inputParams.get(PARAMETER_NAME_TIME);
                String resolution = (String) inputParams.get(PARAMETER_NAME_RESOLUTION);
                String timesone = (String) inputParams.get(PARAMETER_NAME_TIMEZONE);

                return getInstant(create, rootNode, time, resolution, timesone);
            }
        };
    }

    private RawIterator<Object[], ProcedureException> getInstant(boolean create, Node rootNode, long time, String resolution, String timesone) {
        Node instant;
        if (!create) {
            try {
                if (rootNode == null) {
                    instant = timeTree.getInstant(time, resolution, timesone);
                } else {
                    instant = timeTree.getInstantWithCustomRoot(rootNode.getId(), time, resolution, timesone);
                }
            } catch (NotFoundException ex) {
                return Iterators.asRawIterator(Collections.<Object[]>emptyIterator());
            }
        } else if (rootNode == null) {
            instant = timeTree.getOrCreateInstant(time, resolution, timesone);
        } else {
            instant = timeTree.getOrCreateInstantWithCustomRoot(rootNode.getId(), time, resolution, timesone);
        }
        return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[]{instant}).iterator());
    }

    public CallableProcedure.BasicProcedure getInstants() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("range"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_INPUT, Neo4jTypes.NTMap)
                .out(PARAMETER_NAME_INSTANTS, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                validateRangeParameters(input[0]);
                Map<String, Object> inputParams = (Map) input[0];
                boolean create = (boolean) inputParams.getOrDefault(PARAMETER_NAME_CREATE, false);
                Node rootNode = (Node) inputParams.getOrDefault(PARAMETER_NAME_ROOT, null);
                long startTime = (long) inputParams.get(PARAMETER_NAME_START_TIME);
                long endTime = (long) inputParams.get(PARAMETER_NAME_END_TIME);
                String resolution = (String) inputParams.get(PARAMETER_NAME_RESOLUTION);
                String timesone = (String) inputParams.get(PARAMETER_NAME_TIMEZONE);

                List<Node> instants;
                if (!create) {
                    try {
                        if (rootNode == null) {
                            instants = timeTree.getInstants(startTime, endTime, resolution, timesone);
                        } else {
                            instants = timeTree.getInstantsWithCustomRoot(rootNode.getId(), startTime, endTime, resolution, timesone);
                        }
                    } catch (NotFoundException ex) {
                        return Iterators.asRawIterator(Collections.<Object[]>emptyIterator());
                    }
                } else if (rootNode == null) {
                    instants = timeTree.getOrCreateInstants(startTime, endTime, resolution, timesone);
                } else {
                    instants = timeTree.getOrCreateInstantsWithCustomRoot(rootNode.getId(), startTime, endTime, resolution, timesone);
                }

                Set<Object[]> result = new HashSet<>();
                instants.stream().forEach((node) -> result.add(new Object[]{node}));
                return Iterators.asRawIterator(result.iterator());
            }

        };
    }

    
    public CallableProcedure.BasicProcedure now() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("now"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_INPUT, Neo4jTypes.NTMap)
                .out(PARAMETER_NAME_INSTANT, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                validateSingleParamter(input[0]);
                Map<String, Object> inputParams = (Map) input[0];
                boolean create = (boolean) inputParams.getOrDefault(PARAMETER_NAME_CREATE, false);
                Node rootNode = (Node) inputParams.getOrDefault(PARAMETER_NAME_ROOT, null);
                String resolution = (String) inputParams.get(PARAMETER_NAME_RESOLUTION);
                String timesone = (String) inputParams.get(PARAMETER_NAME_TIMEZONE);

                return getInstant(create, rootNode, System.currentTimeMillis(), resolution, timesone);
            }
        };
    }

    protected void validateSingleParamter(Object object) {
        checkIsMap(object);
        Map<String, Object> inputParams = (Map) object;
        checkCreate(inputParams);
        checkTime(inputParams, PARAMETER_NAME_TIME);
    }

    protected void validateRangeParameters(Object object) {
        checkIsMap(object);
        Map<String, Object> inputParams = (Map) object;
        checkCreate(inputParams);
        checkTime(inputParams, PARAMETER_NAME_START_TIME);
        checkTime(inputParams, PARAMETER_NAME_END_TIME);
    }
    
    protected static ProcedureSignature.ProcedureName getProcedureName(String procedureName) {
        return procedureName("ga", "timetree", procedureName);
    }
}
