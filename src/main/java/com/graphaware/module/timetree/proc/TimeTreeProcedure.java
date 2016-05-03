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

    private static final String PARAMETER_NAME_TIME = "time";
    private static final String PARAMETER_NAME_RESOLUTION = "resolution";
    private static final String PARAMETER_NAME_TIMEZONE = "timezone";
    private static final String PARAMETER_NAME_INSTANT = "instant";
    private static final String PARAMETER_NAME_STAR_TIME = "starTime";
    private static final String PARAMETER_NAME_END_TIME = "endTime";
    private static final String PARAMETER_NAME_INSTANTS = "instants";
    private static final String PARAMETER_NAME_ROOT = "root";

    private final TimeTreeBusinessLogic timeTree;

    public TimeTreeProcedure(GraphDatabaseService database) {
        timeTree = new TimeTreeBusinessLogic(database);

    }

    public CallableProcedure.BasicProcedure getOrCreateInstant() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("mergeInstant"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANT, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input) throws ProcedureException {
                Node instant = timeTree.getOrCreateInstant((long) input[0], (String) input[1], (String) input[2]);
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[]{instant}).iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getInstant() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("single"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANT, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                Node instant;
                try {
                    instant = timeTree.getInstant((long) input[0], (String) input[1], (String) input[2]);
                } catch (NotFoundException ex) {
                    return Iterators.asRawIterator(Collections.<Object[]>emptyIterator());
                }
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[]{instant}).iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getInstants() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("range"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_STAR_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_END_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANTS, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                List<Node> instants = timeTree.getInstants((long) input[0], (long) input[1], (String) input[2], (String) input[3]);
                Set<Object[]> result = new HashSet<>();
                instants.stream().forEach((node) -> result.add(new Object[]{node}));
                return Iterators.asRawIterator(result.iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getOrCreateInstants() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("mergeInstants"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_STAR_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_END_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANTS, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                List<Node> instants = timeTree.getOrCreateInstants((long) input[0], (long) input[1], (String) input[2], (String) input[3]);
                Set<Object[]> result = new HashSet<>();
                instants.stream().forEach((node) -> result.add(new Object[]{node}));
                return Iterators.asRawIterator(result.iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getInstantWithCustomRoot() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("singleWithRoot"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_ROOT, Neo4jTypes.NTNode)
                .in(PARAMETER_NAME_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANT, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                Node instant;
                try {
                    instant = timeTree.getInstantWithCustomRoot(((Node) input[0]).getId(), (long) input[1], (String) input[2], (String) input[3]);
                } catch (NotFoundException ex) {
                    return Iterators.asRawIterator(Collections.<Object[]>emptyIterator());
                }
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[]{instant}).iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getOrCreateInstantWithCustomRoot() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("mergeInstantWithRoot"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_ROOT, Neo4jTypes.NTNode)
                .in(PARAMETER_NAME_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANT, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                Node instant = timeTree.getOrCreateInstantWithCustomRoot(((Node) input[0]).getId(), (long) input[1], (String) input[2], (String) input[3]);
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[]{instant}).iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getInstantsWithCustomRoot() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("rangeWithRoot"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_ROOT, Neo4jTypes.NTNode)
                .in(PARAMETER_NAME_STAR_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_END_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANTS, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                List<Node> instants = timeTree.getInstantsWithCustomRoot(((Node) input[0]).getId(), (long) input[1], (long) input[2], (String) input[3], (String) input[4]);
                Set<Object[]> result = new HashSet<>();
                instants.stream().forEach((node) -> result.add(new Object[]{node}));
                return Iterators.asRawIterator(result.iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure getOrCreateInstantsWithCustomRoot() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("mergeInstantsWithRoot"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_ROOT, Neo4jTypes.NTNode)
                .in(PARAMETER_NAME_STAR_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_END_TIME, Neo4jTypes.NTNumber)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANTS, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                List<Node> instants = timeTree.getOrCreateInstantsWithCustomRoot(((Node) input[0]).getId(), (long) input[1], (long) input[2], (String) input[3], (String) input[4]);
                Set<Object[]> result = new HashSet<>();
                instants.stream().forEach((node) -> result.add(new Object[]{node}));
                return Iterators.asRawIterator(result.iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure now() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("now"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANT, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                Node instant;
                try {
                    instant = timeTree.getInstant(System.currentTimeMillis(), (String) input[0], (String) input[1]);
                } catch (NotFoundException ex) {
                    return Iterators.asRawIterator(Collections.<Object[]>emptyIterator());
                }
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[]{instant}).iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure mergeNow() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("mergeNow"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANT, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                Node instant = timeTree.getOrCreateInstant(System.currentTimeMillis(), (String) input[0], (String) input[1]);
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[]{instant}).iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure nowWithRoot() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("nowWithRoot"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_ROOT, Neo4jTypes.NTNode)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANT, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                Node instant;
                try {
                    instant = timeTree.getInstantWithCustomRoot(((Node) input[0]).getId(), System.currentTimeMillis(), (String) input[1], (String) input[2]);
                } catch (NotFoundException ex) {
                    return Iterators.asRawIterator(Collections.<Object[]>emptyIterator());
                }
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[]{instant}).iterator());
            }
        };
    }

    public CallableProcedure.BasicProcedure mergeNowWithRoot() {
        return new CallableProcedure.BasicProcedure(procedureSignature(getProcedureName("mergeNowWithRoot"))
                .mode(ProcedureSignature.Mode.READ_WRITE)
                .in(PARAMETER_NAME_ROOT, Neo4jTypes.NTNode)
                .in(PARAMETER_NAME_RESOLUTION, Neo4jTypes.NTString)
                .in(PARAMETER_NAME_TIMEZONE, Neo4jTypes.NTString)
                .out(PARAMETER_NAME_INSTANT, Neo4jTypes.NTNode).build()) {

            @Override
            public RawIterator<Object[], ProcedureException> apply(CallableProcedure.Context ctx, Object[] input) throws ProcedureException {
                Node instant = timeTree.getOrCreateInstantWithCustomRoot(((Node) input[0]).getId(), System.currentTimeMillis(), (String) input[1], (String) input[2]);
                return Iterators.asRawIterator(Collections.<Object[]>singleton(new Object[]{instant}).iterator());
            }
        };
    }

    private static ProcedureSignature.ProcedureName getProcedureName(String procedureName) {
        return procedureName("ga", "timetree", procedureName);
    }

}
