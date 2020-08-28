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
package com.graphaware.module.timetree.module;

import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;

import static com.graphaware.module.timetree.domain.Resolution.MONTH;
import static org.neo4j.graphdb.Label.label;

/**
 * Unit test for
 * {@link com.graphaware.module.timetree.module.TimeTreeConfiguration}
 */
public class TimeTreeModuleSerializationTest {

    @Test
    public void testRestartingDatabase() throws IOException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        temporaryFolder.getRoot().deleteOnExit();

        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase(new File(temporaryFolder.getRoot().getAbsolutePath()));

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration().withAutoAttach(true), database));
        runtime.start();
        runtime.waitUntilStarted();

        database.shutdown();

        database = new GraphDatabaseFactory().newEmbeddedDatabase(new File(temporaryFolder.getRoot().getAbsolutePath()));

        runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration().withResolution(MONTH).withAutoAttach(true), database));
        runtime.start();
        runtime.waitUntilStarted();

        database.shutdown();

        temporaryFolder.delete();
    }
}
