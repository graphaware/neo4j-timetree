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

import com.graphaware.module.timetree.logic.TimeTreeBusinessLogic;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TimeTreeProcedure extends TimeTreeBaseProcedure {

    @Context
    public GraphDatabaseAPI db;

    private TimeTreeBusinessLogic initTimeTree(GraphDatabaseService db) {
        return new TimeTreeBusinessLogic(db);
    }

    @Procedure(mode = Mode.WRITE, name = "ga.timetree.single")
    @Description(value = "CALL ga.timetree.single({time: 1463659567468}) yield instant return instant")
    public Stream<Instant> single(@Name("params") Map<String, Object> params) {
        validateSingleParameter(params);

        boolean create = (boolean) params.getOrDefault(PARAMETER_NAME_CREATE, false);
        Node rootNode = (Node) params.getOrDefault(PARAMETER_NAME_ROOT, null);
        long time = (long) params.get(PARAMETER_NAME_TIME);
        String resolution = (String) params.get(PARAMETER_NAME_RESOLUTION);
        String timezone = (String) params.get(PARAMETER_NAME_TIMEZONE);

        return getInstant(create, rootNode, time, resolution, timezone);
    }

    @Procedure(mode = Mode.WRITE, name = "ga.timetree.merge")
    @Description(value = "")
    public Stream<Instant> merge(@Name("params") Map<String, Object> params) {
        validateSingleParameter(params);
        boolean create = true;
        Node rootNode = (Node) params.getOrDefault(PARAMETER_NAME_ROOT, null);
        long time = (long) params.get(PARAMETER_NAME_TIME);
        String resolution = (String) params.get(PARAMETER_NAME_RESOLUTION);
        String timezone = (String) params.get(PARAMETER_NAME_TIMEZONE);

        return getInstant(create, rootNode, time, resolution, timezone);
    }

    @Procedure(mode = Mode.WRITE, name = "ga.timetree.now")
    @Description(value = "CALL ga.timetree.now({}) yield instant return instant")
    public Stream<Instant> now(@Name("params") Map<String, Object> params) {
        checkCreate(params);
        boolean create = (boolean) params.getOrDefault(PARAMETER_NAME_CREATE, false);
        Node rootNode = (Node) params.getOrDefault(PARAMETER_NAME_ROOT, null);
        String resolution = (String) params.get(PARAMETER_NAME_RESOLUTION);
        String timezone = (String) params.get(PARAMETER_NAME_TIMEZONE);

        return getInstant(create, rootNode, System.currentTimeMillis(), resolution, timezone);
    }

    @Procedure(mode = Mode.WRITE, name = "ga.timetree.range")
    @Description(value = "CALL ga.timetree.range({start: 1463659567468, end: 1463859569504, create: true}) yield instant return instant")
    public Stream<Instant> range(@Name("params") Map<String, Object> params) {
        validateRangeParameters(params);
        boolean create = (boolean) params.getOrDefault(PARAMETER_NAME_CREATE, false);
        Node rootNode = (Node) params.getOrDefault(PARAMETER_NAME_ROOT, null);
        long startTime = (long) params.get(PARAMETER_NAME_START_TIME);
        long endTime = (long) params.get(PARAMETER_NAME_END_TIME);
        String resolution = (String) params.get(PARAMETER_NAME_RESOLUTION);
        String timezone = (String) params.get(PARAMETER_NAME_TIMEZONE);

        final TimeTreeBusinessLogic timeTree = initTimeTree(db);

        List<Node> instants;
        if (!create) {
            try {
                if (rootNode == null) {
                    instants = timeTree.getInstants(startTime, endTime, resolution, timezone);
                } else {
                    instants = timeTree.getInstantsWithCustomRoot(rootNode.getId(), startTime, endTime, resolution, timezone);
                }
            } catch (NotFoundException ex) {
                return Stream.empty();
            }
        } else if (rootNode == null) {
            instants = timeTree.getOrCreateInstants(startTime, endTime, resolution, timezone);
        } else {
            instants = timeTree.getOrCreateInstantsWithCustomRoot(rootNode.getId(), startTime, endTime, resolution, timezone);
        }

        return instants.stream().map(Instant::new);
    }

    private Stream<Instant> getInstant(boolean create, Node rootNode, long time, String resolution, String timezone) {
        final TimeTreeBusinessLogic timeTree = initTimeTree(db);

        Node instant;
        if (!create) {
            try {
                if (rootNode == null) {
                    instant = timeTree.getInstant(time, resolution, timezone);
                } else {
                    instant = timeTree.getInstantWithCustomRoot(rootNode.getId(), time, resolution, timezone);
                }
            } catch (NotFoundException ex) {
                return null;
            }
        } else if (rootNode == null) {
            instant = timeTree.getOrCreateInstant(time, resolution, timezone);
        } else {
            instant = timeTree.getOrCreateInstantWithCustomRoot(rootNode.getId(), time, resolution, timezone);
        }

        return Stream.of(new Instant(instant));
    }

    protected void validateSingleParameter(Object object) {
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
}
