/*
 * Copyright (c) 2013-2019 GraphAware
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

import static com.graphaware.module.timetree.domain.Resolution.MONTH;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.runtime.config.FluentRuntimeConfiguration;
import com.graphaware.runtime.metadata.DefaultTxDrivenModuleMetadata;
import com.graphaware.runtime.metadata.GraphPropertiesMetadataRepository;
import com.graphaware.runtime.metadata.ModuleMetadata;
import com.graphaware.runtime.metadata.ModuleMetadataRepository;
import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import static org.neo4j.graphdb.Label.label;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * Unit test for
 * {@link com.graphaware.module.timetree.module.TimeTreeConfiguration}
 */
public class TimeTreeModuleSerializationTest {

    private static final Label Email = label("Email");
    private static final Label Event = label("Event");
    private static final Label CustomRoot = label("CustomRoot");
    private static final long TIMESTAMP;
    private ModuleMetadataRepository repository;

    static {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        c.set(2015, Calendar.APRIL, 5, 12, 55, 22);
        TIMESTAMP = c.getTimeInMillis();
    }

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
    
    @Test
    public void testStore() throws IOException {

        
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        temporaryFolder.getRoot().deleteOnExit();

        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase(new File(temporaryFolder.getRoot().getAbsolutePath()));
        
        repository = new GraphPropertiesMetadataRepository(database, FluentRuntimeConfiguration.defaultConfiguration(database), "TEST");

        ModuleMetadata metadata = new DefaultTxDrivenModuleMetadata(TimeTreeConfiguration.defaultConfiguration().withAutoAttach(true));

        repository.persistModuleMetadata("TEST", metadata);

        assertEquals(metadata, repository.getModuleMetadata("TEST"));

        database.shutdown();

        temporaryFolder.delete();
    }
}
