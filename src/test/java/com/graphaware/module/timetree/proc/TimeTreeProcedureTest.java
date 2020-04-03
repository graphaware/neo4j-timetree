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

package com.graphaware.module.timetree.proc;

import com.graphaware.test.integration.GraphAwareIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.Label.label;

/**
 * Procedure test for {@link com.graphaware.module.timetree.proc.TimeTreeProcedure}.
 */
public class TimeTreeProcedureTest extends GraphAwareIntegrationTest {

    @Test
    public void testGetOrCreateInstant() {
        long dateInMillis = dateToMillis(2013, 5, 5);
        Map<String, Object> params = new HashMap<>();
        params.put("time", dateInMillis);
        params.put("resolution", null);
        params.put("timezone", null);
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.single({time: {time}, resolution: {resolution}, timezone: {timezone}, create: true}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            resIterator.stream().forEach((node) -> assertEquals(node.getProperty("value"), 5));
            tx.success();
        }

    }

    @Test
    public void testGetInstant() {
        long dateInMillis = dateToMillis(2013, 5, 4);
        Map<String, Object> params = new HashMap<>();
        params.put("time", dateInMillis);
        params.put("resolution", null);
        params.put("timezone", null);
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.single({time: {time}, resolution: {resolution}, timezone: {timezone}}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

    }

    @Test
    public void testNow() {
        Map<String, Object> params = new HashMap<>();
        params.put("resolution", null);
        params.put("timezone", null);
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.now({resolution: {resolution}, timezone: {timezone}}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.now({resolution: {resolution}, timezone: {timezone}, create: true}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
            resIterator.stream().forEach((node) -> assertEquals(node.getProperty("value"), dayOfMonth));
            tx.success();
        }
    }

    @Test
    public void testNowWithCustomRoot() {
        Map<String, Object> params = new HashMap<>();
        params.put("resolution", null);
        params.put("timezone", null);
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.now({root: n, resolution: {resolution}, timezone: {timezone}}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.now({root: n, resolution: {resolution}, timezone: {timezone}, create: true}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(label("CustomRoot"));
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.now({root: n, resolution: {resolution}, timezone: {timezone}, create: true}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
            resIterator.stream().forEach((node) -> assertEquals(node.getProperty("value"), dayOfMonth));
            tx.success();
        }

    }

    @Test
    public void testGetInstants() {
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);
        Map<String, Object> params = new HashMap<>();
        params.put("startTime", startDateInMillis);
        params.put("endTime", endDateInMillis);
        params.put("resolution", null);
        params.put("timezone", null);
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.range({start: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(label("CustomRoot"));
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

    }

    @Test
    public void testGetOrCreateInstants() {
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);
        Map<String, Object> params = new HashMap<>();
        params.put("startTime", startDateInMillis);
        params.put("endTime", endDateInMillis);
        params.put("resolution", null);
        params.put("timezone", null);
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.range({start: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}, create: true}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            long count = resIterator.stream().count();
            assertEquals(4, count);
            tx.success();
        }

    }

    @Test
    public void testGetOrCreateInstantsWrongParams() {
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);
        Map<String, Object> params = new HashMap<>();
        params.put("startTime", startDateInMillis);
        params.put("endTime", endDateInMillis);
        params.put("resolution", null);
        params.put("timezone", null);
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.range({stat: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}, create: true}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            long count = resIterator.stream().count();
            fail("This should be unreached");
            tx.success();
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage().contains("No parameter start specified"));
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.range({start: {startTime}, en: {endTime}, resolution: {resolution}, timezone: {timezone}, create: true}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            long count = resIterator.stream().count();
            fail("This should be unreached");
            tx.success();
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage().contains("No parameter end specified"));
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.range({start: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}, create: 'yeah'}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            long count = resIterator.stream().count();
            fail("This should be unreached");
            tx.success();
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage().contains("java.lang.String cannot be cast to java.lang.Boolean"));
        }

    }

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequestedWithCustomRoot() {
        long dateInMillis = dateToMillis(2013, 5, 4);
        Map<String, Object> params = new HashMap<>();
        params.put("time", dateInMillis);
        params.put("resolution", null);
        params.put("timezone", null);
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}, resolution: {resolution}, timezone: {timezone}}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(label("CustomRoot"));
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}, resolution: {resolution}, timezone: {timezone}}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}, resolution: {resolution}, timezone: {timezone}, create: true}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            resIterator.stream().forEach((node) -> assertEquals(node.getProperty("value"), 4));
            tx.success();
        }

        assertSameGraph(getDatabase(), "CREATE" +
                "(root:CustomRoot)," +
                "(root)-[:FIRST]->(year:Year {value:2013})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:5})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:4})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)");

    }

    @Test
    public void consecutiveDaysShouldBeCreatedWhenRequestedWithCustomRoot() {
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);
        Map<String, Object> params = new HashMap<>();
        params.put("startTime", startDateInMillis);
        params.put("endTime", endDateInMillis);
        params.put("resolution", null);
        params.put("timezone", null);
        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}, create: true}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(label("CustomRoot"));
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}, create: true}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            long count = resIterator.stream().count();
            assertEquals(4, count);
            tx.success();
        }


        assertSameGraph(getDatabase(), "CREATE" +
                "(root:CustomRoot)," +
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
    }

    private long dateToMillis(int year, int month, int day) {
        return dateToDateTime(year, month, day).getMillis();
    }

    private DateTime dateToDateTime(int year, int month, int day) {
        return new DateTime(year, month, day, 0, 0, DateTimeZone.UTC);
    }


}
