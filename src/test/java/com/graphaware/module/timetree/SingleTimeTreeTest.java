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
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.List;
import java.util.TimeZone;

import static com.graphaware.module.timetree.domain.Resolution.DAY;
import static com.graphaware.module.timetree.domain.Resolution.MILLISECOND;
import static com.graphaware.module.timetree.SingleTimeTree.VALUE_PROPERTY;
import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequested() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        TimeInstant timeInstant =  TimeInstant.instant(dateInMillis);
        
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

    @Test(expected = NotFoundException.class)
    public void whenTheRootIsDeletedSubsequentRestApiCallsShouldThrowNotFoundException() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);
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


        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateInMillis));
            tx.success();
        }
        //NotFoundException should be thrown
    }

    @Test
    public void whenTheRootIsDeletedAndCacheInvalidatedTimeInstantShouldBeCreated() {
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
        } catch (NotFoundException nfe) {
            timeTree.invalidateCaches();
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

    private long dateToMillis(int year, int month, int day) {
        return dateToDateTime(year, month, day).getMillis();
    }

    private DateTime dateToDateTime(int year, int month, int day) {
        return new DateTime(year, month, day, 0, 0, UTC);
    }
}
