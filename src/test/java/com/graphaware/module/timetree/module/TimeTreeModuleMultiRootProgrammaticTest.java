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

package com.graphaware.module.timetree.module;

import com.graphaware.common.kv.GraphKeyValueStore;
import com.graphaware.common.kv.KeyValueStore;
import com.graphaware.common.policy.BaseNodeInclusionPolicy;
import com.graphaware.common.serialize.Serializer;
import com.graphaware.module.timetree.domain.Resolution;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.runtime.metadata.DefaultTxDrivenModuleMetadata;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;

import static com.graphaware.module.timetree.domain.Resolution.MONTH;
import com.graphaware.test.integration.EmbeddedDatabaseIntegrationTest;
import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import java.io.File;

/**
 * Test for {@link TimeTreeModule} set up programatically.
 */
public class TimeTreeModuleMultiRootProgrammaticTest extends EmbeddedDatabaseIntegrationTest {

    private static final Label Email = DynamicLabel.label("Email");
    private static final Label Event = DynamicLabel.label("Event");
    private static final Label CustomRoot = DynamicLabel.label("CustomRoot");
    private static final long TIMESTAMP;

    static {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        c.set(2015, Calendar.APRIL, 5, 12, 55, 22);
        TIMESTAMP = c.getTimeInMillis();
    }

    @Test
    public void withNoModuleNoTreeIsCreated() {
        createEvent(createCustomRoot());

        assertSameGraph(getDatabase(), "CREATE (:CustomRoot {name:'CustomRoot'}), (:Event {subject:'Neo4j', timeTreeRootId:0, timestamp:" + TIMESTAMP + "})");
    }

    @Test
    public void noTreeIsCreatedWhenEventDoesNotMatchInclusionPolicy() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        long customRootId = createCustomRoot();
        createEvent(customRootId, Email);

        assertSameGraph(getDatabase(), "CREATE (:CustomRoot {name:'CustomRoot'}), (:Email {subject:'Neo4j', timeTreeRootId:0, timestamp:" + TIMESTAMP + "})");
    }

    @Test
    public void noTreeIsCreatedWhenEventDoesNotHaveTimestamp() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        long customRootId = createCustomRoot();

        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Event);
            node.setProperty("subject", "Neo4j");
            node.setProperty("timeTreeRootId", customRootId);
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE (:CustomRoot {name:'CustomRoot'}), (:Event {subject:'Neo4j', timeTreeRootId: 0})");
    }

    @Test
    public void noTreeIsCreatedWhenEventDoesNotHaveValidTimestamp() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        long customRootId = createCustomRoot();

        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Event);
            node.setProperty("timestamp", "invalid");
            node.setProperty("subject", "Neo4j");
            node.setProperty("timeTreeRootId", customRootId);
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE (:CustomRoot {name:'CustomRoot'}), (:Event {subject:'Neo4j', timestamp:'invalid', timeTreeRootId:0})");
    }

    @Test
    public void shouldAttachEventWithDefaultConfig() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        long customRootId = createCustomRoot();

        createEvent(customRootId);

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timeTreeRootId:0, timestamp:" + TIMESTAMP + "})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
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
    public void shouldAttachEventWithDefaultConfigSingleTx() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        try (Transaction tx = getDatabase().beginTx()) {
            createEvent(createCustomRoot());
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timeTreeRootId:0, timestamp:" + TIMESTAMP + "})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
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

        createEvent(createCustomRoot(), Event, Email);

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event:Email {subject:'Neo4j', timeTreeRootId:0,timestamp:" + TIMESTAMP + "})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
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
                        .withCustomTimeTreeRootProperty("rootId")
                        .withTimestampProperty("time")
                        .with(new BaseNodeInclusionPolicy() {
                            @Override
                            public boolean include(Node node) {
                                return node.hasLabel(Email);
                            }
                        })
                        .withRelationshipType(DynamicRelationshipType.withName("SENT_AT"))
                        .withTimeZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+1")))
                        .withTimestampProperty("time")
                        .withResolution(Resolution.MINUTE)
                ,
                getDatabase()));
        runtime.start();

        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Email);
            node.setProperty("subject", "Neo4j");
            node.setProperty("time", TIMESTAMP);
            node.setProperty("rootId", createCustomRoot());
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Email {subject:'Neo4j', rootId:1, time:" + TIMESTAMP + "})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
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
            node.setProperty("timeTreeRootId", createCustomRoot());
            eventId = node.getId();
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().getNodeById(eventId).setProperty("timestamp", TIMESTAMP);
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timeTreeRootId:1, timestamp:" + TIMESTAMP + "})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
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
            node.setProperty("timeTreeRootId", createCustomRoot());
            eventId = node.getId();
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().getNodeById(eventId).removeProperty("timestamp");
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timeTreeRootId:1})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
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
    public void shouldAttachEventWithRemovedRootIdToSingleTree() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        long eventId;
        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Event);
            node.setProperty("subject", "Neo4j");
            node.setProperty("timestamp", TIMESTAMP);
            node.setProperty("timeTreeRootId", createCustomRoot());
            eventId = node.getId();
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().getNodeById(eventId).removeProperty("timeTreeRootId");
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + "})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day), "+

                        "(sroot:TimeTreeRoot)," +
                        "(sroot)-[:FIRST]->(syear:Year {value:2015})," +
                        "(sroot)-[:CHILD]->(syear)," +
                        "(sroot)-[:LAST]->(syear)," +
                        "(syear)-[:FIRST]->(smonth:Month {value:4})," +
                        "(syear)-[:CHILD]->(smonth)," +
                        "(syear)-[:LAST]->(smonth)," +
                        "(smonth)-[:FIRST]->(sday:Day {value:5})," +
                        "(smonth)-[:CHILD]->(sday)," +
                        "(smonth)-[:LAST]->(sday), " +
                        "(sday)<-[:AT_TIME]-(event)"
        );
    }

    @Test
    public void shouldUnAttachEventWithRemovedTimestampAndRootId() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        long eventId;
        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Event);
            node.setProperty("subject", "Neo4j");
            node.setProperty("timestamp", TIMESTAMP);
            node.setProperty("timeTreeRootId", createCustomRoot());
            eventId = node.getId();
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().getNodeById(eventId).removeProperty("timestamp");
            getDatabase().getNodeById(eventId).removeProperty("timeTreeRootId");
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j'})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
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
        createEvent(createCustomRoot());

        assertSameGraph(getDatabase(), "CREATE (root:CustomRoot {name:'CustomRoot'}), (:Event {subject: 'Neo4j', timeTreeRootId:0, timestamp:" + TIMESTAMP + "})");

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration().withAutoAttach(true), getDatabase()));
        runtime.start();

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j',timeTreeRootId:0, timestamp:" + TIMESTAMP + "})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
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
    public void shouldNotAttachExistingEventsWhenModuleRegisteredForTheFirstTimeWithAutoAttachEnabledButEventsAlreadyAttached() {

        getDatabase().execute("CREATE " +
                "(event:Event {subject:'Neo4j', timeTreeRootId:1, timestamp:" + TIMESTAMP + "})," +
                "(root:CustomRoot {name:'CustomRoot'})," +
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
                        "(event:Event {subject:'Neo4j', timeTreeRootId:1, timestamp:" + TIMESTAMP + "})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
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
        createEvent(createCustomRoot());

        assertSameGraph(getDatabase(), "CREATE (root:CustomRoot {name:'CustomRoot'}), (:Event {subject:'Neo4j', timeTreeRootId:0, timestamp:" + TIMESTAMP + "})");

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        assertSameGraph(getDatabase(), "CREATE (root:CustomRoot {name:'CustomRoot'}), (:Event {subject:'Neo4j', timeTreeRootId:0, timestamp:" + TIMESTAMP + "})");
    }

    @Test
    public void shouldNotAttachAnythingWhenModuleHasNotBeenRunningForAWhile() {
        createEvent(createCustomRoot());

        assertSameGraph(getDatabase(), "CREATE (root:CustomRoot {name:'CustomRoot'}), (:Event {subject:'Neo4j', timeTreeRootId:0, timestamp:" + TIMESTAMP + "})");

        KeyValueStore keyValueStore = new GraphKeyValueStore(getDatabase());
        try (Transaction tx = getDatabase().beginTx()) {
            keyValueStore.set("_GA_TX_MODULE_timetree", Serializer.toByteArray(new DefaultTxDrivenModuleMetadata(TimeTreeConfiguration.defaultConfiguration())));
            tx.success();
        }

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        assertSameGraph(getDatabase(), "CREATE (root:CustomRoot {name:'CustomRoot'}), (:Event {subject:'Neo4j', timeTreeRootId:0, timestamp:" + TIMESTAMP + "})");
    }

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

        long customRoot;
        try (Transaction tx1 = database.beginTx()) {
            Node node1 = database.createNode(CustomRoot);
            customRoot = node1.getId();
            node1.setProperty("name", "CustomRoot");
            tx1.success();
        }

        try (Transaction tx = database.beginTx()) {
            Node node = database.createNode(Event);
            node.setProperty("subject", "Neo4j");
            node.setProperty("timestamp", TIMESTAMP);
            node.setProperty("timeTreeRootId", customRoot);
            tx.success();
        }

        assertSameGraph(database, "CREATE " +
                        "(event:Event {subject:'Neo4j', timeTreeRootId:0, timestamp:" + TIMESTAMP + "})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
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
                        "(event:Event {subject:'Neo4j', timeTreeRootId:0, timestamp:" + TIMESTAMP + "})," +
                        "(root:CustomRoot {name:'CustomRoot'})," +
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

    private void createEvent(long rootId) {
        createEvent(rootId, Event);
    }

    private void createEvent(long rootId, Label... labels) {
        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(labels);
            node.setProperty("subject", "Neo4j");
            node.setProperty("timestamp", TIMESTAMP);
            node.setProperty("timeTreeRootId", rootId);
            tx.success();
        }
    }

    private long createCustomRoot() {
        Long id;
        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(CustomRoot);
            id = node.getId();
            node.setProperty("name", "CustomRoot");
            tx.success();
        }
        return id;
    }
}
