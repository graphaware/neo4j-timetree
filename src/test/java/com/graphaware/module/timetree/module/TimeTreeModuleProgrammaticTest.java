/*
 * Copyright (c) 2015 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.timetree.module;

import com.graphaware.common.kv.GraphKeyValueStore;
import com.graphaware.common.kv.KeyValueStore;
import com.graphaware.common.policy.NodeInclusionPolicy;
import com.graphaware.common.policy.NodePropertyInclusionPolicy;
import com.graphaware.common.serialize.Serializer;
import com.graphaware.module.timetree.domain.Resolution;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.runtime.metadata.DefaultTxDrivenModuleMetadata;
import com.graphaware.test.integration.DatabaseIntegrationTest;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.neo4j.graphdb.*;

import java.util.Calendar;
import java.util.TimeZone;

import static com.graphaware.test.unit.GraphUnit.assertSameGraph;

/**
 * Test for {@link com.graphaware.module.timetree.module.TimeTreeModule} set up programatically.
 */
public class TimeTreeModuleProgrammaticTest extends DatabaseIntegrationTest {

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
    public void shouldAttachEventForCustomRootWithDefaultConfig() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), getDatabase()));
        runtime.start();

        Long customRootId;
        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(CustomRoot);
            customRootId = node.getId();
            tx.success();
        }
        createEventWithRootIdProperty(customRootId, Event);

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Event {subject:'Neo4j', timestamp:" + TIMESTAMP + ", timeTreeRootId:" + customRootId + "})," +
                        "(root:CustomRoot)," +
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
                        .with(new NodeInclusionPolicy() {
                            @Override
                            public boolean include(Node node) {
                                return node.hasLabel(Email);
                            }
                        })
                        .with(new NodePropertyInclusionPolicy() {
                            @Override
                            public boolean include(String key, Node propertyContainer) {
                                return "time".equals(key);
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

    private void createEventWithRootIdProperty(Long rootId, Label... labels) {
        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(labels);
            node.setProperty("subject", "Neo4j");
            node.setProperty("timestamp", TIMESTAMP);
            node.setProperty("timeTreeRootId", rootId);
            tx.success();
        }
    }
}
