/*
 * Copyright (c) 2014 GraphAware
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

package com.graphaware.module.timetree;

import com.graphaware.module.timetree.domain.Event;
import com.graphaware.module.timetree.domain.Resolution;
import com.graphaware.module.timetree.domain.TimeInstant;
import com.graphaware.test.integration.DatabaseIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static com.graphaware.module.timetree.domain.Resolution.*;
import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link com.graphaware.module.timetree.SingleTimeTree}.
 */
public class TimedEventsTest extends DatabaseIntegrationTest {

    private static final DynamicRelationshipType AT_TIME = DynamicRelationshipType.withName("AT_TIME");
    
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
            List<Event> events = timedEvents.getEvents(timeInstant, DynamicRelationshipType.withName("NONEXISTENT_REL"));
            assertEquals(0, events.size());

            events = timedEvents.getEvents(timeInstant, AT_TIME);
            assertEquals(1, events.size());
            assertEquals("eventA", events.get(0).getNode().getProperty("name"));
            assertEquals("AT_TIME", events.get(0).getRelationshipType().name());
            tx.success();
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
            List<Event> events = timedEvents.getEvents(timeInstant1, timeInstant2, AT_TIME);
            assertEquals(2, events.size());
            assertEquals("eventA", events.get(0).getNode().getProperty("name"));
            assertEquals("eventB", events.get(1).getNode().getProperty("name"));

            events = timedEvents.getEvents(timeInstant1, timeInstant2, DynamicRelationshipType.withName("NONEXISTENT_RELATION"));
            assertEquals(0, events.size());

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
