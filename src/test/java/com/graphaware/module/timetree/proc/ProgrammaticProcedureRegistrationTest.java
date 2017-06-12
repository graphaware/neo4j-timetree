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

import com.graphaware.module.timetree.module.TimeTreeConfiguration;
import com.graphaware.module.timetree.module.TimeTreeModule;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

public class ProgrammaticProcedureRegistrationTest {

    @Test
    public void canRegisterProceduresProgrammatically() throws Exception {
        GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), database));
        runtime.start();

        ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(TimeTreeProcedure.class);

        database.execute("CALL ga.timetree.now({})");
    }
}
