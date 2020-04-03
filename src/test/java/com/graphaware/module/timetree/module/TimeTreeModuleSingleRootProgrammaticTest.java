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

import com.graphaware.common.kv.GraphKeyValueStore;
import com.graphaware.common.kv.KeyValueStore;
import com.graphaware.common.policy.inclusion.BaseNodeInclusionPolicy;
import com.graphaware.common.serialize.Serializer;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.runtime.metadata.DefaultTxDrivenModuleMetadata;
import com.graphaware.test.integration.EmbeddedDatabaseIntegrationTest;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static com.graphaware.module.timetree.domain.Resolution.MINUTE;
import static com.graphaware.module.timetree.domain.Resolution.MONTH;
import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.neo4j.graphdb.Label.label;

/**
 * Test for {@link com.graphaware.module.timetree.module.TimeTreeModule} set up programatically.
 */
public class TimeTreeModuleSingleRootProgrammaticTest extends EmbeddedDatabaseIntegrationTest {

    private static final Label Email = label("Email");
    private static final Label Event = label("Event");
    private static final long TIMESTAMP;

    static {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        c.set(2015, Calendar.APRIL, 5, 12, 55, 22);
        TIMESTAMP = c.getTimeInMillis();
    }

    @Test
    public void withNoModuleNoTreeIsCreated() {
        createEvent();

        assertSameGraph(getDatabase(), "CREATE (:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})");
    }

    @Test
    public void noTreeIsCreatedWhenEventDoesNotMatchInclusionPolicy() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        createEvent(Email);

        assertSameGraph(getDatabase(), "CREATE (:Email {subject:'Neo4j', timestamp:" + TIMESTAMP + "})");
    }

    @Test
    public void noTreeIsCreatedWhenEventDoesNotHaveTimestamp() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Event);
            node.setProperty("subject", "Neo4j");
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE (:Event {subject:'Neo4j'})");
    }

    @Test
    public void noTreeIsCreatedWhenEventDoesNotHaveValidTimestamp() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Event);
            node.setProperty("timestamp", "invalid");
            node.setProperty("subject", "Neo4j");
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE (:Event {subject:'Neo4j', timestamp:'invalid'})");
    }

    @Test
    public void shouldAttachEventWithDefaultConfig() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        createEvent();

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)," +
                        "(day)<-[:AT_TIME]-(event)"
        );
    }

    @Test
    public void shouldAttachEventWithMultipleLabels() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        createEvent(Event, Email);

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event:Email {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)," +
                        "(day)<-[:AT_TIME]-(event)"
        );
    }

    @Test
    public void shouldAttachEventWithCustomConfig() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree",
                TimeTreeConfiguration
                        .defaultConfiguration()
                        .withTimestampProperty("time")
                        .with(new BaseNodeInclusionPolicy() {
                            @Override
                            public boolean include(Node node) {
                                return node.hasLabel(Email);
                            }
                        })
                        .withRelationshipType(RelationshipType.withName("SENT_AT"))
                        .withTimeZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+1")))
                        .withTimestampProperty("time")
                        .withResolution(MINUTE)
                ,
                getDatabase()));
        runtime.start();

        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Email);
            node.setProperty("subject", "Neo4j");
            node.setProperty("time", TIMESTAMP);
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Email {subject:'Neo4j', time:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)," +
                        "(day)-[:FIRST]->(hour:Hour {value:13})," + //1 hour more!
                        "(day)-[:CHILD]->(hour)," +
                        "(day)-[:LAST]->(hour)," +
                        "(hour)-[:FIRST]->(minute:Minute {value:55})," +
                        "(hour)-[:CHILD]->(minute)," +
                        "(hour)-[:LAST]->(minute)," +
                        "(minute)<-[:SENT_AT]-(event)"
        );
    }

    @Test
    public void shouldReAttachEventWithChangedTimestamp() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        long eventId;
        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Event);
            node.setProperty("subject", "Neo4j");
            node.setProperty("timestamp", 1426238522920L);
            eventId = node.getId();
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().getNodeById(eventId).setProperty("timestamp", TIMESTAMP);
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:CHILD]->(month3:Month {value:3})," +
                        "(year)-[:CHILD]->(month4:Month {value:4})," +
                        "(year)-[:FIRST]->(month3)," +
                        "(year)-[:LAST]->(month4)," +
                        "(month4)<-[:NEXT]-(month3)," +
                        "(month4)-[:FIRST]->(day5:Day {value:5})," +
                        "(month4)-[:CHILD]->(day5)," +
                        "(month4)-[:LAST]->(day5)," +
                        "(month3)-[:FIRST]->(day13:Day {value:13})," +
                        "(month3)-[:CHILD]->(day13)," +
                        "(month3)-[:LAST]->(day13)," +
                        "(day5)<-[:NEXT]-(day13)," +
                        "(day5)<-[:AT_TIME]-(event)"
        );
    }

    @Test //issue #38
    public void shouldReAttachEventWithCreatedTimestamp() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        long eventId;
        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Event);
            node.setProperty("subject", "Neo4j");
            eventId = node.getId();
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().getNodeById(eventId).setProperty("timestamp", TIMESTAMP);
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:CHILD]->(month4:Month {value:4})," +
                        "(year)-[:FIRST]->(month4)," +
                        "(year)-[:LAST]->(month4)," +
                        "(month4)-[:FIRST]->(day5:Day {value:5})," +
                        "(month4)-[:CHILD]->(day5)," +
                        "(month4)-[:LAST]->(day5)," +
                        "(day5)<-[:AT_TIME]-(event)"
        );
    }

    @Test
    public void shouldUnAttachEventWithRemovedTimestamp() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        long eventId;
        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Event);
            node.setProperty("subject", "Neo4j");
            node.setProperty("timestamp", TIMESTAMP);
            eventId = node.getId();
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().getNodeById(eventId).removeProperty("timestamp");
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j'})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)"
        );
    }

    @Test
    public void shouldAttachExistingEventsWhenModuleRegisteredForTheFirstTimeWithAutoAttachEnabled() {
        createEvent();

        assertSameGraph(getDatabase(), "CREATE (:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})");

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration().withAutoAttach(true), getDatabase()));
        runtime.start();

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)," +
                        "(day)<-[:AT_TIME]-(event)"
        );
    }

    @Test
    public void shouldAttachExistingEventsWhenModuleRegisteredForTheFirstTimeWithAutoAttachEnabled2() {
        createEvent();

        assertSameGraph(getDatabase(), "CREATE (:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})");

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree",
                TimeTreeConfiguration
                        .defaultConfiguration()
                        .with(IncludeEvents.getInstance())
                        .withAutoAttach(true), getDatabase()));
        runtime.start();

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)," +
                        "(day)<-[:AT_TIME]-(event)"
        );
    }

    @Test
    public void shouldAttachExistingEventsWhenModuleRegisteredForTheFirstTimeWithAutoAttachEnabled3() {
        createEvent();

        assertSameGraph(getDatabase(), "CREATE (:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})");

        Map<String, String> config = new HashMap<>();
        config.put("event", "hasLabel('Event')");
        config.put("relationship", "SENT_ON");
        config.put("autoAttach", "true");

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModuleBootstrapper().bootstrapModule("timetree", config, getDatabase()));
        runtime.start();

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)," +
                        "(day)<-[:SENT_ON]-(event)"
        );
    }

    @Test
    public void shouldAttachExistingEventsWhenModuleRegisteredForTheFirstTimeWithAutoAttachEnabled4() {
        createEvent();

        assertSameGraph(getDatabase(), "CREATE (:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})");

        Map<String, String> config = new HashMap<>();
        config.put("event", "com.graphaware.module.timetree.module.IncludeEvents");
        config.put("relationship", "SENT_ON");
        config.put("autoAttach", "true");

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModuleBootstrapper().bootstrapModule("timetree", config, getDatabase()));
        runtime.start();

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)," +
                        "(day)<-[:SENT_ON]-(event)"
        );
    }

    @Test
    public void shouldNotAttachExistingEventsWhenModuleRegisteredForTheFirstTimeWithAutoAttachEnabledButEventsAlreadyAttached() {

        getDatabase().execute("CREATE " +
                "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:2015})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:4})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:5})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)," +
                "(day)<-[:AT_TIME]-(event)");

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration().withAutoAttach(true), getDatabase()));
        runtime.start();

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)," +
                        "(day)<-[:AT_TIME]-(event)"
        );
    }

    @Test
    public void shouldNotAttachExistingEventsWhenModuleRegisteredForTheFirstTimeWithDefaultConfig() {
        createEvent();

        assertSameGraph(getDatabase(), "CREATE (:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})");

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        assertSameGraph(getDatabase(), "CREATE (:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})");
    }

    @Test
    public void shouldNotAttachAnythingWhenModuleHasNotBeenRunningForAWhile() {
        createEvent();

        assertSameGraph(getDatabase(), "CREATE (:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})");

        KeyValueStore keyValueStore = new GraphKeyValueStore(getDatabase());
        try (Transaction tx = getDatabase().beginTx()) {
            keyValueStore.set("_GA_TX_MODULE_timetree", Serializer.toByteArray(new DefaultTxDrivenModuleMetadata(TimeTreeConfiguration.defaultConfiguration())));
            tx.success();
        }

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        assertSameGraph(getDatabase(), "CREATE (:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})");
    }

    //issue #29
    @Test
    public void shouldRecreateTreeWhenResolutionChanges() throws IOException {
        getDatabase().shutdown();

        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        temporaryFolder.getRoot().deleteOnExit();

        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase(new File(temporaryFolder.getRoot().getAbsolutePath()));

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration().withAutoAttach(true), database));
        runtime.start();
        runtime.waitUntilStarted();

        try (Transaction tx = database.beginTx()) {
            Node node = database.createNode(Event);
            node.setProperty("subject", "Neo4j");
            node.setProperty("timestamp", TIMESTAMP);
            tx.success();
        }

        assertSameGraph(database, "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)," +
                        "(day)<-[:AT_TIME]-(event)"
        );

        database.shutdown();

        database = new GraphDatabaseFactory().newEmbeddedDatabase(new File(temporaryFolder.getRoot().getAbsolutePath()));

        runtime = GraphAwareRuntimeFactory.createRuntime(database);
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration().withResolution(MONTH).withAutoAttach(true), database));
        runtime.start();
        runtime.waitUntilStarted();

        assertSameGraph(database, "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)," +
                        "(month)<-[:AT_TIME]-(event)"
        );

        database.shutdown();

        temporaryFolder.delete();
    }

    private void createEvent() {
        createEvent(Event);
    }

    private void createEvent(Label... labels) {
        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(labels);
            node.setProperty("subject", "Neo4j");
            node.setProperty("timestamp", TIMESTAMP);
            tx.success();
        }
    }
}
