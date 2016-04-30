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

package com.graphaware.module.timetree;

import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.domain.Resolution;
import com.graphaware.module.timetree.domain.TimeInstant;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import java.util.*;

import static com.graphaware.module.timetree.domain.Resolution.MONTH;
import static com.graphaware.module.timetree.domain.Resolution.YEAR;
import com.graphaware.test.integration.ServerIntegrationTest;
import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.Iterables.count;

/**
 * Unit test for {@link com.graphaware.module.timetree.TimeTreeBackedEvents}.
 */
public class TimeTreeBackedEventsTest extends ServerIntegrationTest {

    private static final RelationshipType AT_TIME = withName("AT_TIME");
    private static final RelationshipType AT_OTHER_TIME = withName("AT_OTHER_TIME");
    private static final Set<RelationshipType> REL_TYPES = new HashSet<>(Arrays.asList(AT_TIME, AT_OTHER_TIME));

    private TimedEvents timedEvents;

    private static final DateTimeZone UTC = DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"));

    @Before
    public void setUp() throws Exception {
        super.setUp();
        timedEvents = new TimeTreeBackedEvents(new SingleTimeTree(getDatabase()));
    }

    @Test
    public void eventAndTimeInstantShouldBeCreatedWhenEventIsAttached() {
        //Given
        DateTime now = DateTime.now(UTC);
        TimeInstant timeInstant = TimeInstant.now();
        Node event;

        try (Transaction tx = getDatabase().beginTx()) {
            event = getDatabase().createNode();
            event.setProperty("name", "eventA");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event, AT_TIME, timeInstant);
            tx.success();
        }

        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:" + now.getYear() + "})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:" + now.getMonthOfYear() + "})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:" + now.getDayOfMonth() + "})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)," +
                "(day)<-[:AT_TIME]-(event {name:'eventA'})");
    }

    @Test
    public void eventAndTimeInstantShouldBeCreatedWhenEventIsAttachedInOppositeDirection() {
        //Given
        DateTime now = DateTime.now(UTC);
        TimeInstant timeInstant = TimeInstant.now();
        Node event;

        try (Transaction tx = getDatabase().beginTx()) {
            event = getDatabase().createNode();
            event.setProperty("name", "eventA");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event, AT_TIME, OUTGOING, timeInstant);
            tx.success();
        }

        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:" + now.getYear() + "})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:" + now.getMonthOfYear() + "})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:" + now.getDayOfMonth() + "})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)," +
                "(day)-[:AT_TIME]->(event {name:'eventA'})");
    }

    @Test(expected = IllegalArgumentException.class)
    public void eventShouldNotBeAttachedWithDirectionBoth() {
        //Given
        DateTime now = DateTime.now(UTC);
        TimeInstant timeInstant = TimeInstant.now();
        Node event;

        try (Transaction tx = getDatabase().beginTx()) {
            event = getDatabase().createNode();
            event.setProperty("name", "eventA");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event, AT_TIME, BOTH, timeInstant);
            tx.success();
        }
    }

    @Test //https://github.com/graphaware/neo4j-timetree/issues/13
    public void eventShouldNotBeAttachedTwiceInTheSameTx() {
        //Given
        DateTime now = DateTime.now(UTC);
        TimeInstant timeInstant = TimeInstant.now();
        Node event;

        try (Transaction tx = getDatabase().beginTx()) {
            event = getDatabase().createNode();
            event.setProperty("name", "eventA");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            assertTrue(timedEvents.attachEvent(event, AT_TIME, timeInstant));
            assertFalse(timedEvents.attachEvent(event, AT_TIME, timeInstant));
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            assertEquals(1, count(event.getRelationships(OUTGOING, AT_TIME)));
            tx.success();
        }
    }

    @Test //https://github.com/graphaware/neo4j-timetree/issues/13
    public void eventShouldNotBeAttachedTwiceInDifferentTx() {
        //Given
        DateTime now = DateTime.now(UTC);
        TimeInstant timeInstant = TimeInstant.now();
        Node event;

        try (Transaction tx = getDatabase().beginTx()) {
            event = getDatabase().createNode();
            event.setProperty("name", "eventA");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            assertTrue(timedEvents.attachEvent(event, AT_TIME, timeInstant));
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            assertFalse(timedEvents.attachEvent(event, AT_TIME, timeInstant));
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            assertEquals(1, count(event.getRelationships(OUTGOING, AT_TIME)));
            tx.success();
        }
    }


    @Test
    public void multipleEventsAndTimeInstantShouldBeCreatedWhenEventIsAttached() {
        //Given
        DateTime now = DateTime.now(UTC);
        TimeInstant timeInstant = TimeInstant.now();

        Node event1, event2;

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event1, AT_TIME, timeInstant);
            timedEvents.attachEvent(event2, AT_TIME, timeInstant);
            tx.success();
        }

        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:" + now.getYear() + "})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:" + now.getMonthOfYear() + "})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:" + now.getDayOfMonth() + "})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)," +
                "(day)<-[:AT_TIME]-(event1 {name:'eventA'})," +
                "(day)<-[:AT_TIME]-(event2 {name:'eventB'})");
    }

    @Test
    public void multipleEventsAndTimeInstantsShouldBeCreatedWhenEventsAreAttached() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1));
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 3));
        Node event1, event2;

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event1, AT_TIME, timeInstant1);
            timedEvents.attachEvent(event2, AT_TIME, timeInstant2);
            tx.success();
        }

        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:2012})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:11})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:1})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:CHILD]->(day2:Day {value:3})," +
                "(day)-[:NEXT]->(day2)," +
                "(month)-[:LAST]->(day2)," +
                "(day)<-[:AT_TIME]-(event1 {name:'eventA'})," +
                "(day2)<-[:AT_TIME]-(event2 {name:'eventB'})");
    }

    @Test
    public void eventShouldBeFetchedForATimeInstant() {
        //Given
        TimeInstant timeInstant = TimeInstant.now();

        Node event;

        try (Transaction tx = getDatabase().beginTx()) {
            event = getDatabase().createNode();
            event.setProperty("name", "eventA");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event, AT_TIME, timeInstant);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(timeInstant);
            assertEquals(1, events.size());
            assertEquals("eventA", events.get(0).getNode().getProperty("name"));
            assertEquals("AT_TIME", events.get(0).getRelationshipType().name());
            tx.success();
        }
    }

    @Test
    public void allEventsShouldBeFetchedForATimeInstant() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1));
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 3));
        TimeInstant timeInstant3 = TimeInstant.instant(dateToMillis(2012, 11, 5));
        TimeInstant timeInstant4 = TimeInstant.instant(dateToMillis(2012, 11, 5)).with(MONTH);
        TimeInstant timeInstant5 = TimeInstant.instant(dateToMillis(2012, 12, 1));

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(createEvent("eventA"), AT_TIME, timeInstant1);
            timedEvents.attachEvent(createEvent("eventB"), AT_TIME, timeInstant2);
            timedEvents.attachEvent(createEvent("eventC"), AT_TIME, timeInstant2);
            timedEvents.attachEvent(createEvent("eventD"), AT_TIME, timeInstant3);
            timedEvents.attachEvent(createEvent("eventE"), AT_TIME, timeInstant4);
            timedEvents.attachEvent(createEvent("eventF"), AT_TIME, timeInstant4);
            timedEvents.attachEvent(createEvent("eventG"), AT_TIME, timeInstant5);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(timeInstant4);
            assertEquals(6, events.size());
            assertEquals("eventA", events.get(0).getNode().getProperty("name"));
            assertEquals("eventB", events.get(2).getNode().getProperty("name"));
            assertEquals("eventC", events.get(1).getNode().getProperty("name"));
            assertEquals("eventD", events.get(3).getNode().getProperty("name"));
            assertEquals("eventE", events.get(5).getNode().getProperty("name"));
            assertEquals("eventF", events.get(4).getNode().getProperty("name"));
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(timeInstant4.with(YEAR));
            assertEquals(7, events.size());
            assertEquals("eventA", events.get(0).getNode().getProperty("name"));
            assertEquals("eventB", events.get(2).getNode().getProperty("name"));
            assertEquals("eventC", events.get(1).getNode().getProperty("name"));
            assertEquals("eventD", events.get(3).getNode().getProperty("name"));
            assertEquals("eventE", events.get(5).getNode().getProperty("name"));
            assertEquals("eventF", events.get(4).getNode().getProperty("name"));
            assertEquals("eventG", events.get(6).getNode().getProperty("name"));
            tx.success();
        }
    }

    private Node createEvent(String name) {
        Node node = getDatabase().createNode();
        node.setProperty("name", name);
        return node;
    }

    @Test
    public void multipleEventsShouldBeFetchedForATimeInstant() {
        //Given
        TimeInstant timeInstant = TimeInstant.now();
        Node event1, event2;

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event1, AT_TIME, timeInstant);
            timedEvents.attachEvent(event2, AT_TIME, timeInstant);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<String> eventNames = new ArrayList<>();
            List<Event> events = timedEvents.getEvents(timeInstant);
            assertEquals(2, events.size());
            eventNames.add((String) events.get(0).getNode().getProperty("name"));
            eventNames.add((String) events.get(1).getNode().getProperty("name"));
            assertTrue(eventNames.contains("eventA"));
            assertTrue(eventNames.contains("eventB"));
            tx.success();
        }
    }

    @Test
    public void eventShouldBeFetchedForARelationAndTimeInstant() {
        //Given
        TimeInstant timeInstant = TimeInstant.now();
        Node event;

        try (Transaction tx = getDatabase().beginTx()) {
            event = getDatabase().createNode();
            event.setProperty("name", "eventA");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event, AT_TIME, timeInstant);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(timeInstant, Collections.<RelationshipType>singleton(withName("NONEXISTENT_REL")));
            assertEquals(0, events.size());

            events = timedEvents.getEvents(timeInstant, Collections.singleton(AT_TIME));
            assertEquals(1, events.size());
            assertEquals("eventA", events.get(0).getNode().getProperty("name"));
            assertEquals("AT_TIME", events.get(0).getRelationshipType().name());
            tx.success();
        }

    }

    @Test
    public void eventShouldBeFetchedForMultipleRelationsAndTimeInstant() {
        //Given
        TimeInstant timeInstant = TimeInstant.now();
        Node event1, event2;

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");

            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event1, AT_TIME, timeInstant);
            timedEvents.attachEvent(event2, AT_OTHER_TIME, timeInstant);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            Set<RelationshipType> relationships = new HashSet<>();
            relationships.add(AT_TIME);
            relationships.add(AT_OTHER_TIME);
            List<Event> events = timedEvents.getEvents(timeInstant, relationships);
            assertEquals(2, events.size());
        }

    }

    @Test
    public void eventsShouldBeFetchedForTimeRange() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1));
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 3));

        Node event1, event2;

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event1, AT_TIME, timeInstant1);
            timedEvents.attachEvent(event2, AT_TIME, timeInstant2);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(timeInstant1, timeInstant2);
            assertEquals(2, events.size());
            assertEquals("eventA", events.get(0).getNode().getProperty("name"));
            assertEquals("eventB", events.get(1).getNode().getProperty("name"));
            tx.success();
        }
    }

    @Test
    public void eventsShouldBeFetchedForARelationAndTimeRange() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1));
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 3));

        Node event1, event2;

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event1, AT_TIME, timeInstant1);
            timedEvents.attachEvent(event2, AT_TIME, timeInstant2);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(timeInstant1, timeInstant2, Collections.singleton(AT_TIME));
            assertEquals(2, events.size());
            assertEquals("eventA", events.get(0).getNode().getProperty("name"));
            assertEquals("eventB", events.get(1).getNode().getProperty("name"));

            events = timedEvents.getEvents(timeInstant1, timeInstant2, Collections.<RelationshipType>singleton(withName("NONEXISTENT_RELATION")));
            assertEquals(0, events.size());

            tx.success();
        }
    }

    @Test
    public void eventsShouldBeFetchedForMultipleRelationsAndTimeRange() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1));
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 3));

        Node event1, event2, event3;

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            event3 = getDatabase().createNode();
            event3.setProperty("name", "eventC");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event1, AT_TIME, timeInstant1);
            timedEvents.attachEvent(event2, AT_TIME, timeInstant2);
            timedEvents.attachEvent(event3, AT_OTHER_TIME, timeInstant1);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(timeInstant1, timeInstant2, REL_TYPES);
            assertEquals(3, events.size());
            assertEquals("eventC", events.get(0).getNode().getProperty("name"));
            assertEquals("eventA", events.get(1).getNode().getProperty("name"));
            assertEquals("eventB", events.get(2).getNode().getProperty("name"));

            tx.success();
        }
    }

    @Test
    public void eventsShouldBeFetchedForMultipleRelationsAndTimeRange2() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1));
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 2));
        TimeInstant timeInstant3 = TimeInstant.instant(dateToMillis(2012, 11, 3));

        Node event1, event2, event3;

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            event3 = getDatabase().createNode();
            event3.setProperty("name", "eventC");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event1, AT_TIME, timeInstant1);
            timedEvents.attachEvent(event2, AT_TIME, timeInstant2);
            timedEvents.attachEvent(event3, AT_OTHER_TIME, timeInstant3);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(timeInstant1, timeInstant3, REL_TYPES);
            assertEquals(3, events.size());
            assertEquals("eventA", events.get(0).getNode().getProperty("name"));
            assertEquals("eventB", events.get(1).getNode().getProperty("name"));
            assertEquals("eventC", events.get(2).getNode().getProperty("name"));

            tx.success();
        }
    }

    @Test
    public void eventsShouldBeFetchedForMillisecondResolutionTree() { //Test for Issue #2
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1)).with(Resolution.MILLISECOND);
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 2)).with(Resolution.MILLISECOND);

        Node event1, event2;

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event1, AT_TIME, timeInstant1);
            timedEvents.attachEvent(event2, AT_TIME, timeInstant2);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(timeInstant1, timeInstant2, Collections.singleton(AT_TIME));
            assertEquals(2, events.size());
            assertEquals("eventA", events.get(0).getNode().getProperty("name"));
            assertEquals("eventB", events.get(1).getNode().getProperty("name"));

            events = timedEvents.getEvents(timeInstant1, timeInstant2, Collections.<RelationshipType>singleton(withName("NONEXISTENT_RELATION")));
            assertEquals(0, events.size());

            tx.success();
        }
    }

    @Test
    public void noEventsShouldBeFetchedFromEmptyTree() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1)).with(Resolution.MILLISECOND);
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 2)).with(Resolution.MILLISECOND);

        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(timeInstant1, timeInstant2, Collections.singleton(AT_TIME));
            assertEquals(0, events.size());

            tx.success();
        }
    }

    @Test
    public void eventsShouldBeFetchedWhenThereIsOnlyOneInstant() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1)).with(Resolution.MILLISECOND);
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 2)).with(Resolution.MILLISECOND);

        Node event1, event2;

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event1, AT_TIME, timeInstant1);
            timedEvents.attachEvent(event2, AT_TIME, timeInstant1);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events;
            events = timedEvents.getEvents(timeInstant1, timeInstant2, Collections.singleton(AT_TIME));
            assertEquals(2, events.size());
            assertTrue("eventA".equals(events.get(0).getNode().getProperty("name")) || "eventA".equals(events.get(1).getNode().getProperty("name")));
            assertTrue("eventB".equals(events.get(0).getNode().getProperty("name")) || "eventA".equals(events.get(1).getNode().getProperty("name")));

            events = timedEvents.getEvents(timeInstant1, timeInstant2, Collections.<RelationshipType>singleton(withName("NONEXISTENT_RELATION")));
            assertEquals(0, events.size());

            tx.success();
        }
    }

    @Test
    public void noEventsShouldBeFetchedWhenThereAreNoEventsInTheRange() {   //Issue #9
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 2)).with(Resolution.MILLISECOND);
        TimeInstant zero = TimeInstant.instant(0l).with(Resolution.MILLISECOND);
        TimeInstant beforeTimeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1)).with(Resolution.MILLISECOND);
        Node event1;

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timedEvents.attachEvent(event1, AT_TIME, timeInstant1);
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(zero, beforeTimeInstant1);
            assertEquals(0, events.size());

            tx.success();
        }
    }

    @Test
    @Ignore //save my CPU from frying
    public void perSecondEventsShouldBeFetched() { //Test for Issue #2
        //Given an event every second for 6 hours
        Calendar runningCal = new GregorianCalendar(2014, Calendar.OCTOBER, 11, 0, 0, 0);
        Calendar endCal = new GregorianCalendar(2014, Calendar.OCTOBER, 11, 5, 59, 59);

        Calendar beforeStartCal = new GregorianCalendar(2014, Calendar.OCTOBER, 10, 0, 0, 0);
        Calendar afterEndCal = new GregorianCalendar(2014, Calendar.OCTOBER, 13, 0, 59, 59);


        TimeInstant beforeStartTime = TimeInstant.instant(beforeStartCal.getTime().getTime()).with(Resolution.MILLISECOND);
        TimeInstant afterEndTime = TimeInstant.instant(afterEndCal.getTime().getTime()).with(Resolution.MILLISECOND);

        //When
        int count = 0;
        try (Transaction tx = getDatabase().beginTx()) {
            while (runningCal.before(endCal)) {
                System.out.println("Creating event " + count);
                Node event = getDatabase().createNode();
                event.setProperty("name", "event" + count++);
                TimeInstant runningTime = TimeInstant.instant(runningCal.getTime().getTime()).with(Resolution.MILLISECOND);
                timedEvents.attachEvent(event, AT_TIME, runningTime);
                runningCal.add(Calendar.SECOND, 1);
            }
            tx.success();
        }

        //Then
        System.out.println("Fetching events");

        try (Transaction tx = getDatabase().beginTx()) {
            List<Event> events = timedEvents.getEvents(beforeStartTime, afterEndTime, Collections.singleton(AT_TIME)); //Make sure that events are still returned within this range
            assertEquals(count, events.size());
            assertEquals("event0", events.get(0).getNode().getProperty("name"));
            assertEquals("event" + (count - 1), events.get(count - 1).getNode().getProperty("name"));
            tx.success();
        }
    }

    private long dateToMillis(int year, int month, int day) {
        return dateToDateTime(year, month, day).getMillis();
    }

    private DateTime dateToDateTime(int year, int month, int day) {
        return new DateTime(year, month, day, 0, 0, UTC);
    }
}
