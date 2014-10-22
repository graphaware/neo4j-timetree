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

import com.graphaware.common.util.PropertyContainerUtils;
import com.graphaware.module.timetree.domain.TimeInstant;
import com.graphaware.module.timetree.domain.TimeTreeLabels;
import com.graphaware.test.integration.DatabaseIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.List;
import java.util.TimeZone;

import static com.graphaware.module.timetree.SingleTimeTree.VALUE_PROPERTY;
import static com.graphaware.module.timetree.domain.Resolution.*;
import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

/**
 * Unit test for {@link SingleTimeTree}.
 */
public class SingleTimeTreeTest extends DatabaseIntegrationTest {

    private TimeTree timeTree; //class under test

    private static final DateTimeZone UTC = DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"));

    @Before
    public void setUp() throws Exception {
        super.setUp();
        timeTree = new SingleTimeTree(getDatabase());
    }

    @Test
    public void nullShouldBeReturnedWhenNonExistingYearIsRequested() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        TimeInstant timeInstant = TimeInstant.instant(dateInMillis).with(YEAR);

        //When
        Node yearNode;
        try (Transaction tx = getDatabase().beginTx()) {
            yearNode = timeTree.getInstant(timeInstant);
            tx.success();
        }

        //Then
        assertNull(yearNode);
        assertSameGraph(getDatabase(), "CREATE (root:TimeTreeRoot)");

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            yearNode = timeTree.getInstantAtOrAfter(timeInstant);
            tx.success();
        }

        //Then
        assertNull(yearNode);
        assertSameGraph(getDatabase(), "CREATE (root:TimeTreeRoot)");

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            yearNode = timeTree.getInstantAtOrBefore(timeInstant);
            tx.success();
        }

        //Then
        assertNull(yearNode);
        assertSameGraph(getDatabase(), "CREATE (root:TimeTreeRoot)");
    }

    @Test
    public void nullShouldBeReturnedWhenNonExistingDayIsRequested() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        TimeInstant timeInstant = TimeInstant.instant(dateInMillis).with(DAY);

        //When
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstant(timeInstant);
            tx.success();
        }

        //Then
        assertNull(dayNode);
        assertSameGraph(getDatabase(), "CREATE (root:TimeTreeRoot)");

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrAfter(timeInstant);
            tx.success();
        }

        //Then
        assertNull(dayNode);
        assertSameGraph(getDatabase(), "CREATE (root:TimeTreeRoot)");

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrBefore(timeInstant);
            tx.success();
        }

        //Then
        assertNull(dayNode);
        assertSameGraph(getDatabase(), "CREATE (root:TimeTreeRoot)");
    }

    @Test
    public void nullShouldBeReturnedWhenNonExistingDayIsRequested2() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        TimeInstant timeInstant = TimeInstant.instant(dateInMillis).with(MONTH);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(timeInstant);
            tx.success();
        }

        timeInstant = TimeInstant.instant(dateInMillis).with(DAY);
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstant(timeInstant);
            tx.success();
        }

        //Then
        assertNull(dayNode);
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:2013})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:5})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)");

        //when
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrAfter(timeInstant);
            tx.success();
        }

        //Then
        assertNull(dayNode);

        //when
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrBefore(timeInstant);
            tx.success();
        }

        //Then
        assertNull(dayNode);
    }

    @Test
    public void nullShouldBeReturnedWhenNonExistingDayIsRequested3() {
        //Given
        TimeInstant timeInstant;

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 5, 4)));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 6, 1)));
            tx.success();
        }

        timeInstant = TimeInstant.instant(dateToMillis(2013, 5, 5));
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstant(timeInstant);
            tx.success();
        }

        //Then
        assertNull(dayNode);
    }

    @Test
    public void previousDayShouldBeReturnedWhenNonExistingDayIsRequested() {
        //Given
        TimeInstant timeInstant;

        Node previous;
        //When
        try (Transaction tx = getDatabase().beginTx()) {
            previous = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 5, 4)));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 6, 1)));
            tx.success();
        }

        timeInstant = TimeInstant.instant(dateToMillis(2013, 5, 5));
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrBefore(timeInstant);
            tx.success();
        }

        //Then
        assertEquals(previous, dayNode);
    }

    @Test
    public void previousDayShouldBeReturnedWhenNonExistingDayIsRequested2() {
        //Given
        TimeInstant timeInstant;

        Node previous;
        //When
        try (Transaction tx = getDatabase().beginTx()) {
            previous = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 4, 20)));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 6, 1)));
            tx.success();
        }

        timeInstant = TimeInstant.instant(dateToMillis(2013, 5, 5));
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrBefore(timeInstant);

            System.out.println("expected:"+PropertyContainerUtils.nodeToString(previous));
            System.out.println("actual:"+PropertyContainerUtils.nodeToString(dayNode));

            tx.success();
        }

        //Then
        assertEquals(previous, dayNode);
    }

    @Test
    public void previousDayShouldBeReturnedWhenNonExistingDayIsRequested3() {
        //Given
        TimeInstant timeInstant;

        Node previous;
        //When
        try (Transaction tx = getDatabase().beginTx()) {
            previous = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2012, 12, 1)));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 6, 1)));
            tx.success();
        }

        timeInstant = TimeInstant.instant(dateToMillis(2013, 5, 5));
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrBefore(timeInstant);

            System.out.println("expected:"+PropertyContainerUtils.nodeToString(previous));
            System.out.println("actual:"+PropertyContainerUtils.nodeToString(dayNode));

            tx.success();
        }

        //Then
        assertEquals(previous, dayNode);
    }

    @Test
    public void previousDayShouldBeReturnedWhenNonExistingDayIsRequested4() {
        //Given
        TimeInstant timeInstant;

        Node previous;
        //When
        try (Transaction tx = getDatabase().beginTx()) {
            previous = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 12, 1)));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2020, 6, 1)));
            tx.success();
        }

        timeInstant = TimeInstant.instant(dateToMillis(2013, 5, 5));
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrBefore(timeInstant);
            tx.success();
        }

        //Then
        assertEquals(previous, dayNode);
    }

    @Test
    public void nextDayShouldBeReturnedWhenNonExistingDayIsRequested() {
        //Given
        TimeInstant timeInstant;

        Node next;
        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 5, 4)));
            next = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 6, 1)));
            tx.success();
        }

        timeInstant = TimeInstant.instant(dateToMillis(2013, 5, 5));
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrAfter(timeInstant);
            tx.success();
        }

        //Then
        assertEquals(next, dayNode);
    }

    @Test
    public void nextDayShouldBeReturnedWhenNonExistingDayIsRequested2() {
        //Given
        TimeInstant timeInstant;

        Node next;
        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 5, 4)));
            next = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 5, 6)));
            tx.success();
        }

        timeInstant = TimeInstant.instant(dateToMillis(2013, 5, 5));
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrAfter(timeInstant);
            tx.success();
        }

        //Then
        assertEquals(next, dayNode);
    }

    @Test
    public void nextDayShouldBeReturnedWhenNonExistingDayIsRequested3() {
        //Given
        TimeInstant timeInstant;

        Node next;
        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 5, 4)));
            next = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2014, 1, 12)));
            tx.success();
        }

        timeInstant = TimeInstant.instant(dateToMillis(2013, 5, 5));
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrAfter(timeInstant);
            tx.success();
        }

        //Then
        assertEquals(next, dayNode);
    }

    @Test
    public void nextDayShouldBeReturnedWhenNonExistingDayIsRequested4() {
        //Given
        TimeInstant timeInstant;

        Node next;
        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 5, 4)));
            next = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2014, 12, 12)));
            tx.success();
        }

        timeInstant = TimeInstant.instant(dateToMillis(2013, 5, 5));
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrAfter(timeInstant);
            tx.success();
        }

        //Then
        assertEquals(next, dayNode);
    }

    @Test
    public void nextDayShouldBeReturnedWhenNonExistingDayIsRequested5() {
        //Given
        TimeInstant timeInstant;

        Node next;
        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 5, 4)));
            next = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2020, 12, 12)));
            tx.success();
        }

        timeInstant = TimeInstant.instant(dateToMillis(2013, 5, 5));
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getInstantAtOrAfter(timeInstant);
            tx.success();
        }

        //Then
        assertEquals(next, dayNode);
    }

    @Test
    public void shouldGetCorrectRangeWithMissingValues() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2013, 5, 4));
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2013, 5, 6));

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(timeInstant1);
            timeTree.getOrCreateInstant(timeInstant2);
            tx.success();
        }

        List<Node> nodes;
        try (Transaction tx = getDatabase().beginTx()) {
            nodes = timeTree.getInstants(timeInstant1, timeInstant2);
            tx.success();
        }

        //Then
        assertEquals(2, nodes.size());

        try (Transaction tx = getDatabase().beginTx()) {
            assertEquals(4, nodes.get(0).getProperty(VALUE_PROPERTY));
            assertEquals(6, nodes.get(1).getProperty(VALUE_PROPERTY));
            tx.success();
        }
    }

    @Test
    public void shouldCreateMissingValues() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2013, 5, 4));
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2013, 5, 6));

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(timeInstant1);
            timeTree.getOrCreateInstant(timeInstant2);
            tx.success();
        }

        List<Node> nodes;
        try (Transaction tx = getDatabase().beginTx()) {
            nodes = timeTree.getOrCreateInstants(timeInstant1, timeInstant2);
            tx.success();
        }

        //Then
        assertEquals(3, nodes.size());

        try (Transaction tx = getDatabase().beginTx()) {
            assertEquals(4, nodes.get(0).getProperty(VALUE_PROPERTY));
            assertEquals(5, nodes.get(1).getProperty(VALUE_PROPERTY));
            assertEquals(6, nodes.get(2).getProperty(VALUE_PROPERTY));
            tx.success();
        }
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequested() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        TimeInstant timeInstant = TimeInstant.instant(dateInMillis);

        //When
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getOrCreateInstant(timeInstant);
            tx.success();
        }

        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:2013})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:5})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:4})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)");

        try (Transaction tx = getDatabase().beginTx()) {
            assertTrue(dayNode.hasLabel(TimeTreeLabels.Day));
            assertEquals(4, dayNode.getProperty(VALUE_PROPERTY));
        }
    }

    @Test
    public void consecutiveDaysShouldBeCreatedWhenRequested() {

        //Given
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);

        TimeInstant startTime = TimeInstant.instant(startDateInMillis).with(DAY).with(UTC);
        TimeInstant endTime = TimeInstant.instant(endDateInMillis).with(DAY).with(UTC);

        //When
        List<Node> dayNodes;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNodes = timeTree.getOrCreateInstants(startTime, endTime);
            tx.success();
        }

        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:2013})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:5})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:CHILD]->(day4:Day {value:4})," +
                "(month)-[:CHILD]->(day5:Day {value:5})," +
                "(month)-[:CHILD]->(day6:Day {value:6})," +
                "(month)-[:CHILD]->(day7:Day {value:7})," +
                "(month)-[:FIRST]->(day4)," +
                "(month)-[:LAST]->(day7)," +
                "(day4)-[:NEXT]->(day5)," +
                "(day5)-[:NEXT]->(day6)," +
                "(day6)-[:NEXT]->(day7)");

        assertEquals(4, dayNodes.size());

        try (Transaction tx = getDatabase().beginTx()) {
            for (int i = 0; i < 4; i++) {
                assertTrue(dayNodes.get(i).hasLabel(TimeTreeLabels.Day));
                assertEquals(i + 4, dayNodes.get(i).getProperty(VALUE_PROPERTY));
            }
        }
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstMilliInstantIsRequested() {
        //Given
        long dateInMillis = new DateTime(2014, 4, 5, 13, 56, 22, 123, UTC).getMillis();
        TimeInstant timeInstant = TimeInstant
                .instant(dateInMillis)
                .with(MILLISECOND)
                .with(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT+1")));

        //When
        Node instantNode;
        try (Transaction tx = getDatabase().beginTx()) {
            instantNode = timeTree.getOrCreateInstant(timeInstant);
            tx.success();
        }

        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:2014})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:4})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:5})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)," +
                "(day)-[:FIRST]->(hour:Hour {value:14})," + //1 hour more!
                "(day)-[:CHILD]->(hour)," +
                "(day)-[:LAST]->(hour)," +
                "(hour)-[:FIRST]->(minute:Minute {value:56})," +
                "(hour)-[:CHILD]->(minute)," +
                "(hour)-[:LAST]->(minute)," +
                "(minute)-[:FIRST]->(second:Second {value:22})," +
                "(minute)-[:CHILD]->(second)," +
                "(minute)-[:LAST]->(second)," +
                "(second)-[:FIRST]->(milli:Millisecond {value:123})," +
                "(second)-[:CHILD]->(milli)," +
                "(second)-[:LAST]->(milli)");

        try (Transaction tx = getDatabase().beginTx()) {
            assertTrue(instantNode.hasLabel(TimeTreeLabels.Millisecond));
            assertEquals(123, instantNode.getProperty(VALUE_PROPERTY));
        }
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenTodayIsRequested() {
        //Given
        DateTime now = DateTime.now(UTC);

        //When
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getOrCreateInstant(TimeInstant.now());
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
                "(month)-[:LAST]->(day)");

        try (Transaction tx = getDatabase().beginTx()) {
            assertTrue(dayNode.hasLabel(TimeTreeLabels.Day));
            assertEquals(now.getDayOfMonth(), dayNode.getProperty(VALUE_PROPERTY));
        }
    }

    @Test
    public void graphShouldNotBeMutatedWhenExistingDayIsRequested() {
        //Given
        DateTime now = DateTime.now(UTC);

        TimeInstant timeInstant = TimeInstant.now().with(DAY);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(timeInstant);
            tx.success();
        }

        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getOrCreateInstant(timeInstant);
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
                "(month)-[:LAST]->(day)");

        try (Transaction tx = getDatabase().beginTx()) {
            assertTrue(dayNode.hasLabel(TimeTreeLabels.Day));
            assertEquals(now.getDayOfMonth(), dayNode.getProperty(VALUE_PROPERTY));
        }
    }

    @Test
    public void fullTreeShouldBeCreatedWhenAFewDaysAreRequested() {
        //Given

        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2012, 11, 1)));
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2012, 11, 10)));
            tx.success();
        }


        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 1, 2)));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 1, 1)));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 1, 4)));
            tx.success();
        }
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 3, 10)));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 2, 1)));
            tx.success();
        }

        //Then
        verifyFullTree();
    }

    @Test
    public void fullTreeShouldBeCreatedWhenAFewDaysAreRequested2() {
        //Given
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 1, 2)));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 1, 4)));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 1, 1)));
            tx.success();
        }
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 2, 1)));
            tx.success();
        }
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2012, 11, 1)));
            tx.success();
        }
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 3, 10)));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2012, 11, 10)));
            tx.success();
        }

        //Then
        verifyFullTree();
    }

    @Test(expected = IllegalArgumentException.class)
    public void getInstantsWithStartTimeAfterEndTimeThrows() {
        //Given
        long startTime = dateToMillis(2013, 1, 2);
        long endTime = dateToMillis(2013, 1, 1);

        // When
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstants(TimeInstant.instant(startTime), TimeInstant.instant(endTime));
            // Then throw
        }
    }

    @Test
    public void whenTheRootIsDeletedTimeInstantShouldBeCreated() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateInMillis));
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            for (Node node : GlobalGraphOperations.at(getDatabase()).getAllNodes()) {
                PropertyContainerUtils.deleteNodeAndRelationships(node);
            }
            tx.success();
        }
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateInMillis));
            tx.success();
        }

        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getOrCreateInstant(TimeInstant.instant(dateInMillis));
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:2013})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:5})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:4})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)");

        try (Transaction tx = getDatabase().beginTx()) {
            assertTrue(dayNode.hasLabel(TimeTreeLabels.Day));
            assertEquals(4, dayNode.getProperty(VALUE_PROPERTY));
        }
    }

    @Test
    public void whenRootExistsItShouldNotBeRecreated() {
        new ExecutionEngine(getDatabase()).execute(
                "CREATE" +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2013})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:5})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:4})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)"
        );

        //Then
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            dayNode = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2013, 5, 4)));
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:2013})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:5})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:4})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)");

        try (Transaction tx = getDatabase().beginTx()) {
            assertTrue(dayNode.hasLabel(TimeTreeLabels.Day));
            assertEquals(4, PropertyContainerUtils.getInt(dayNode, VALUE_PROPERTY));
        }
    }


    private void verifyFullTree() {
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year2012:Year {value:2012})," +
                "(root)-[:LAST]->(year2013:Year {value:2013})," +
                "(root)-[:CHILD]->(year2012)," +
                "(root)-[:CHILD]->(year2013)," +
                "(year2012)-[:NEXT]->(year2013)," +

                "(year2012)-[:FIRST]->(month112012:Month {value:11})," +
                "(year2012)-[:LAST]->(month112012)," +
                "(year2012)-[:CHILD]->(month112012)," +

                "(year2013)-[:CHILD]->(month012013:Month {value:1})," +
                "(year2013)-[:CHILD]->(month022013:Month {value:2})," +
                "(year2013)-[:CHILD]->(month032013:Month {value:3})," +
                "(year2013)-[:FIRST]->(month012013)," +
                "(year2013)-[:LAST]->(month032013)," +
                "(month112012)-[:NEXT]->(month012013)-[:NEXT]->(month022013)-[:NEXT]->(month032013), " +

                "(month112012)-[:CHILD]->(day01112012:Day {value:1})," +
                "(month112012)-[:CHILD]->(day10112012:Day {value:10})," +
                "(month112012)-[:FIRST]->(day01112012)," +
                "(month112012)-[:LAST]->(day10112012)," +
                "(day01112012)-[:NEXT]->(day10112012)-[:NEXT]->(day01012013:Day {value:1})-[:NEXT]->(day02012013:Day {value:2})-[:NEXT]->(day04012013:Day {value:4})-[:NEXT]->(day01022013:Day {value:1})-[:NEXT]->(day10032013:Day {value:10})," +

                "(month012013)-[:FIRST]->(day01012013)," +
                "(month012013)-[:CHILD]->(day01012013)," +
                "(month012013)-[:LAST]->(day04012013)," +
                "(month012013)-[:CHILD]->(day04012013)," +
                "(month012013)-[:CHILD]->(day02012013)," +

                "(month022013)-[:FIRST]->(day01022013)," +
                "(month022013)-[:LAST]->(day01022013)," +
                "(month022013)-[:CHILD]->(day01022013)," +

                "(month032013)-[:CHILD]->(day10032013)," +
                "(month032013)-[:FIRST]->(day10032013)," +
                "(month032013)-[:LAST]->(day10032013)");
    }


    @Test
    public void testDeleteFront() throws Exception {
        try (Transaction tx = getDatabase().beginTx()) {

            Node t1 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 10)));
            Node t2 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 12)));
            Node t3 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 13)));

            Node ev1 = getDatabase().createNode();
            ev1.setProperty("ev", 1);
            t2.createRelationshipTo(ev1, withName("VALUE"));

            Node ev2 = getDatabase().createNode();
            ev2.setProperty("ev", 2);
            t3.createRelationshipTo(ev2, withName("VALUE"));

            timeTree.removeInstant(t1);
            tx.success();
        }
        assertSameGraph(getDatabase(), "create " +
                "(root:TimeTreeRoot)-[:CHILD]->(Year_1:Year {value:2000})," +
                "(root)-[:FIRST]->(Year_1)," +
                "(root)-[:LAST]->(Year_1)," +
                "(Year_1)-[:CHILD]->(Month_1:Month  {value:3})," +
                "(Year_1)-[:FIRST]->(Month_1)," +
                "(Year_1)-[:LAST]->(Month_1)," +
                "(Month_1)-[:CHILD]->(Day_1:Day {value:12})-[:VALUE]->({ev:1})," +
                "(Month_1)-[:CHILD]->(Day_2:Day {value:13})-[:VALUE]->({ev:2})," +
                "(Month_1)-[:FIRST]->(Day_1)," +
                "(Month_1)-[:LAST]->(Day_2)," +
                "(Day_1)-[:NEXT]->(Day_2)");
    }

    @Test
    public void testDeleteMid() throws Exception {
        try (Transaction tx = getDatabase().beginTx()) {

            Node t1 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 10)));
            Node t2 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 12)));
            Node t3 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 13)));

            Node ev1 = getDatabase().createNode();
            ev1.setProperty("ev", 1);
            Node ev3 = getDatabase().createNode();
            ev3.setProperty("ev", 3);


            t1.createRelationshipTo(ev1, withName("VALUE"));
            t3.createRelationshipTo(ev3, withName("VALUE"));
            timeTree.removeInstant(t2);
            tx.success();
        }
        assertSameGraph(getDatabase(), "create " +
                "(root:TimeTreeRoot)-[:CHILD]->(Year_1:Year  {value:2000})," +
                "(root)-[:FIRST]->(Year_1)," +
                "(root)-[:LAST]->(Year_1)," +
                "(Year_1)-[:CHILD]->(Month_1:Month {value:3})," +
                "(Year_1)-[:FIRST]->(Month_1)," +
                "(Year_1)-[:LAST]->(Month_1)," +
                "(Month_1)-[:CHILD]->(Day_1:Day {value:10})-[:VALUE]->({ev:1})," +
                "(Month_1)-[:CHILD]->(Day_2:Day {value:13})-[:VALUE]->({ev:3})," +
                "(Month_1)-[:FIRST]->(Day_1)," +
                "(Month_1)-[:LAST]->(Day_2)," +
                "(Day_1)-[:NEXT]->(Day_2)");
    }

    @Test
    public void testDeleteEnd() throws Exception {
        try (Transaction tx = getDatabase().beginTx()) {

            Node t1 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 10)));
            Node t2 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 12)));
            Node t3 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 13)));

            Node ev1 = getDatabase().createNode();
            ev1.setProperty("ev", 1);
            Node ev2 = getDatabase().createNode();
            ev2.setProperty("ev", 2);

            t1.createRelationshipTo(ev1, withName("VALUE"));
            t2.createRelationshipTo(ev2, withName("VALUE"));
            timeTree.removeInstant(t3);
            tx.success();
        }
        assertSameGraph(getDatabase(), "create " +
                "(root:TimeTreeRoot)-[:CHILD]->(Year_1:Year {value:2000})," +
                "(root)-[:FIRST]->(Year_1)," +
                "(root)-[:LAST]->(Year_1)," +
                "(Year_1)-[:CHILD]->(Month_1:Month  {value:3})," +
                "(Year_1)-[:FIRST]->(Month_1)," +
                "(Year_1)-[:LAST]->(Month_1)," +
                "(Month_1)-[:CHILD]->(Day_1:Day {value:10})-[:VALUE]->({ev:1})," +
                "(Month_1)-[:CHILD]->(Day_2:Day {value:12})-[:VALUE]->({ev:2})," +
                "(Month_1)-[:FIRST]->(Day_1)," +
                "(Month_1)-[:LAST]->(Day_2)," +
                "(Day_1)-[:NEXT]->(Day_2)");
    }

    @Test
    public void testDeleteCross() throws Exception {
        try (Transaction tx = getDatabase().beginTx()) {
            Node t1 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 1, 10)));
            Node ev1 = getDatabase().createNode();
            ev1.setProperty("ev", 1);
            t1.createRelationshipTo(ev1, withName("VALUE"));

            Node t2 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 2, 10)));
            Node ev2 = getDatabase().createNode();
            ev2.setProperty("ev", 2);
            t2.createRelationshipTo(ev2, withName("VALUE"));


            Node t3 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 1, 13)));
            timeTree.removeInstant(t3);

            Node t4 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 1, 14)));
            Node ev3 = getDatabase().createNode();
            ev3.setProperty("ev", 3);

            t4.createRelationshipTo(ev3, withName("VALUE"));
            tx.success();
        }
        assertSameGraph(getDatabase(), "create " +
                "(root:TimeTreeRoot)-[:CHILD]->(Year_1:Year {value:2000})," +
                "(root)-[:FIRST]->(Year_1)," +
                "(root)-[:LAST]->(Year_1)," +
                "(Year_1)-[:CHILD]->(Month_1:Month {value:1})," +
                "(Year_1)-[:CHILD]->(Month_2:Month {value:2})," +
                "(Year_1)-[:FIRST]->(Month_1)," +
                "(Year_1)-[:LAST]->(Month_2)," +
                "(Month_1)-[:CHILD]->(Day_1:Day {value:10})-[:VALUE]->({ev:1})," +
                "(Month_1)-[:CHILD]->(Day_2:Day {value:14})-[:VALUE]->({ev:3})," +
                "(Month_1)-[:FIRST]->(Day_1)," +
                "(Month_1)-[:LAST]->(Day_2)," +
                "(Month_1)-[:NEXT]->(Month_2)," +
                "(Month_2)-[:CHILD]->(Day_3:Day {value:10})-[:VALUE]->({ev:2})," +
                "(Month_2)-[:FIRST]->(Day_3)," +
                "(Month_2)-[:LAST]->(Day_3)," +
                "(Day_1)-[:NEXT]->(Day_2)-[:NEXT]->(Day_3)");
    }

    @Test
    public void testDeleteFrontLv2() throws Exception {
        try (Transaction tx = getDatabase().beginTx()) {
            Node t1 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 1, 10)));
            Node t2 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 2, 12)));
            Node t3 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 13)));

            Node ev2 = getDatabase().createNode();
            ev2.setProperty("ev", 2);
            Node ev3 = getDatabase().createNode();
            ev3.setProperty("ev", 3);

            t2.createRelationshipTo(ev2, withName("VALUE"));
            t3.createRelationshipTo(ev3, withName("VALUE"));

            timeTree.removeInstant(t1);
            tx.success();
        }
        assertSameGraph(getDatabase(), "create " +
                "(root:TimeTreeRoot)-[:CHILD]->(Year_1:Year {value:2000})," +
                "(root)-[:FIRST]->(Year_1)," +
                "(root)-[:LAST]->(Year_1)," +
                "(Year_1)-[:CHILD]->(Month_1:Month {value:2})," +
                "(Year_1)-[:CHILD]->(Month_2:Month {value:3})," +
                "(Year_1)-[:FIRST]->(Month_1)," +
                "(Year_1)-[:LAST]->(Month_2)," +
                "(Month_1)-[:CHILD]->(Day_1:Day {value:12})-[:VALUE]->({ev:2})," +
                "(Month_1)-[:FIRST]->(Day_1)," +
                "(Month_1)-[:LAST]->(Day_1)," +
                "(Month_1)-[:NEXT]->(Month_2)," +
                "(Month_2)-[:CHILD]->(Day_2:Day {value:13})-[:VALUE]->({ev:3})," +
                "(Month_2)-[:FIRST]->(Day_2)," +
                "(Month_2)-[:LAST]->(Day_2)," +
                "(Day_1)-[:NEXT]->(Day_2)");
    }

    @Test
    public void testDeleteMidLv2() throws Exception {
        try (Transaction tx = getDatabase().beginTx()) {
            Node t1 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 1, 10)));
            Node t2 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 2, 12)));
            Node t3 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 13)));

            Node ev1 = getDatabase().createNode();
            ev1.setProperty("ev", 1);
            Node ev3 = getDatabase().createNode();
            ev3.setProperty("ev", 3);

            t1.createRelationshipTo(ev1, withName("VALUE"));
            t3.createRelationshipTo(ev3, withName("VALUE"));

            timeTree.removeInstant(t2);
            tx.success();
        }
        assertSameGraph(getDatabase(), "create " +
                "(root:TimeTreeRoot)-[:CHILD]->(Year_1:Year {value:2000})," +
                "(root)-[:FIRST]->(Year_1)," +
                "(root)-[:LAST]->(Year_1)," +
                "(Year_1)-[:CHILD]->(Month_1:Month {value:1})," +
                "(Year_1)-[:CHILD]->(Month_2:Month {value:3})," +
                "(Year_1)-[:FIRST]->(Month_1)," +
                "(Year_1)-[:LAST]->(Month_2)," +
                "(Month_1)-[:CHILD]->(Day_1:Day {value:10})-[:VALUE]->({ev:1})," +
                "(Month_1)-[:FIRST]->(Day_1)," +
                "(Month_1)-[:LAST]->(Day_1)," +
                "(Month_1)-[:NEXT]->(Month_2)," +
                "(Month_2)-[:CHILD]->(Day_2:Day {value:13})-[:VALUE]->({ev:3})," +
                "(Month_2)-[:FIRST]->(Day_2)," +
                "(Month_2)-[:LAST]->(Day_2)," +
                "(Day_1)-[:NEXT]->(Day_2)");
    }

    @Test
    public void testDeleteEndLv2() throws Exception {
        try (Transaction tx = getDatabase().beginTx()) {
            Node t1 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 1, 10)));
            Node t2 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 2, 12)));
            Node t3 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 13)));

            Node ev1 = getDatabase().createNode();
            ev1.setProperty("ev", 1);
            Node ev2 = getDatabase().createNode();
            ev2.setProperty("ev", 2);

            t1.createRelationshipTo(ev1, withName("VALUE"));
            t2.createRelationshipTo(ev2, withName("VALUE"));

            timeTree.removeInstant(t3);
            tx.success();
        }
        assertSameGraph(getDatabase(), "create " +
                "(root:TimeTreeRoot)-[:CHILD]->(Year_1:Year {value:2000})," +
                "(root)-[:FIRST]->(Year_1)," +
                "(root)-[:LAST]->(Year_1)," +
                "(Year_1)-[:CHILD]->(Month_1:Month {value:1})," +
                "(Year_1)-[:CHILD]->(Month_2:Month {value:2})," +
                "(Year_1)-[:FIRST]->(Month_1)," +
                "(Year_1)-[:LAST]->(Month_2)," +
                "(Month_1)-[:CHILD]->(Day_1:Day {value:10})-[:VALUE]->({ev:1})," +
                "(Month_1)-[:FIRST]->(Day_1)," +
                "(Month_1)-[:LAST]->(Day_1)," +
                "(Month_1)-[:NEXT]->(Month_2)," +
                "(Month_2)-[:CHILD]->(Day_2:Day {value:12})-[:VALUE]->({ev:2})," +
                "(Month_2)-[:FIRST]->(Day_2)," +
                "(Month_2)-[:LAST]->(Day_2)," +
                "(Day_1)-[:NEXT]->(Day_2)");
    }


    @Test
    public void testRemoveAll() throws Exception {
        try (Transaction tx = getDatabase().beginTx()) {
            Node t1 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 1, 10)));
            Node t2 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 2, 12)));
            Node t3 = timeTree.getOrCreateInstant(TimeInstant.instant(dateToMillis(2000, 3, 13)));

            Node ev1 = getDatabase().createNode();
            ev1.setProperty("ev", 1);
            Node ev2 = getDatabase().createNode();
            ev2.setProperty("ev", 2);

            t1.createRelationshipTo(ev1, withName("VALUE"));
            t2.createRelationshipTo(ev2, withName("VALUE"));

            timeTree.removeAll();
            tx.success();
        }
        assertSameGraph(getDatabase(), "create " +
                "({ev:1})," +
                "({ev:2})");
    }

    private long dateToMillis(int year, int month, int day) {
        return dateToDateTime(year, month, day).getMillis();
    }

    private DateTime dateToDateTime(int year, int month, int day) {
        return new DateTime(year, month, day, 0, 0, UTC);
    }
}
