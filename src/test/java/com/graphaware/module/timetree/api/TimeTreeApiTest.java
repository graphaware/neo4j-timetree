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

package com.graphaware.module.timetree.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphaware.common.json.LongIdJsonNode;
import com.graphaware.common.util.EntityUtils;
import com.graphaware.test.integration.GraphAwareIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdb.Label.label;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

/**
 * Integration test for {@link com.graphaware.module.timetree.api.TimeTreeApi}.
 */
public class TimeTreeApiTest extends GraphAwareIntegrationTest {

    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequested() throws JSONException {
        long dateInMillis = dateToMillis(2013, 5, 4);

        Map<String, Object> params = Collections.singletonMap("time", dateInMillis);
        assertNoResults("CALL ga.timetree.single({time: {time}})", params);

        assertEquals("{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", querySingleInstant("CALL ga.timetree.single({time: {time}, create: true})", params), false);

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

        assertEquals("{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", querySingleInstant("CALL ga.timetree.single({time: {time}, create: true})", params), false);
        assertEquals("{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", querySingleInstant("CALL ga.timetree.single({time: {time}, create: false})", params), false);

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
    }

    @Test
    public void consecutiveDaysShouldBeCreatedWhenRequested() throws JSONException {
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);

        assertEquals("[]", queryInstants("CALL ga.timetree.range({start: {startTime}, end: {endTime}})", toMap("startTime", startDateInMillis, "endTime", endDateInMillis)), false);

        assertEquals("[{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]},{\"id\":4,\"properties\":{\"value\":5},\"labels\":[\"Day\"]},{\"id\":5,\"properties\":{\"value\":6},\"labels\":[\"Day\"]},{\"id\":6,\"properties\":{\"value\":7},\"labels\":[\"Day\"]}]",
                queryInstants("CALL ga.timetree.range({start: {startTime}, end: {endTime}, create:true})", toMap("startTime", startDateInMillis, "endTime", endDateInMillis)), false);

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


        assertEquals("[{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]},{\"id\":4,\"properties\":{\"value\":5},\"labels\":[\"Day\"]},{\"id\":5,\"properties\":{\"value\":6},\"labels\":[\"Day\"]},{\"id\":6,\"properties\":{\"value\":7},\"labels\":[\"Day\"]}]",
                queryInstants("CALL ga.timetree.range({start: {startTime}, end: {endTime}, create:true})", toMap("startTime", startDateInMillis, "endTime", endDateInMillis)), false);
        assertEquals("[{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]},{\"id\":4,\"properties\":{\"value\":5},\"labels\":[\"Day\"]},{\"id\":5,\"properties\":{\"value\":6},\"labels\":[\"Day\"]},{\"id\":6,\"properties\":{\"value\":7},\"labels\":[\"Day\"]}]",
                queryInstants("CALL ga.timetree.range({start: {startTime}, end: {endTime}})", toMap("startTime", startDateInMillis, "endTime", endDateInMillis)), false);

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
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequestedWithCustomRoot() throws JSONException {
        long dateInMillis = dateToMillis(2013, 5, 4);

        assertNoResults("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}}) YIELD instant RETURN instant", toMap("time", dateInMillis));
        assertNoResults("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}, create:true}) YIELD instant RETURN instant", toMap("time", dateInMillis));

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(label("CustomRoot"));
            tx.success();
        }

        assertNoResults("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}}) YIELD instant RETURN instant", toMap("time", dateInMillis));
        assertEquals("{\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", querySingleInstant("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}, create:true}) YIELD instant RETURN instant", toMap("time", dateInMillis)), JSONCompareMode.LENIENT);

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


        assertEquals("{\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", querySingleInstant("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}, create:true}) YIELD instant RETURN instant", toMap("time", dateInMillis)), JSONCompareMode.LENIENT);
        assertEquals("{\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", querySingleInstant("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}}) YIELD instant RETURN instant", toMap("time", dateInMillis)), JSONCompareMode.LENIENT);

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
    public void consecutiveDaysShouldBeCreatedWhenRequestedWithCustomRoot() throws JSONException {
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);

        Map<String, Object> params = toMap("startTime", startDateInMillis, "endTime", endDateInMillis);

        assertNoResults("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}, create:true}) YIELD instant RETURN instant", params);
        assertNoResults("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}}) YIELD instant RETURN instant", params);

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(label("CustomRoot"));
            tx.success();
        }

        assertNoResults("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}}) YIELD instant RETURN instant", params);
        String actualStr = queryInstants("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}, create: true}) YIELD instant RETURN instant", params);
        assertEquals("[{\"properties\":{\"value\":4},\"labels\":[\"Day\"]},{\"properties\":{\"value\":5},\"labels\":[\"Day\"]},{\"properties\":{\"value\":6},\"labels\":[\"Day\"]},{\"properties\":{\"value\":7},\"labels\":[\"Day\"]}]", actualStr, JSONCompareMode.LENIENT);

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

        assertEquals("[{\"properties\":{\"value\":4},\"labels\":[\"Day\"]},{\"properties\":{\"value\":5},\"labels\":[\"Day\"]},{\"properties\":{\"value\":6},\"labels\":[\"Day\"]},{\"properties\":{\"value\":7},\"labels\":[\"Day\"]}]", queryInstants("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}, create: true}) YIELD instant RETURN instant", params), JSONCompareMode.LENIENT);
        assertEquals("[{\"properties\":{\"value\":4},\"labels\":[\"Day\"]},{\"properties\":{\"value\":5},\"labels\":[\"Day\"]},{\"properties\":{\"value\":6},\"labels\":[\"Day\"]},{\"properties\":{\"value\":7},\"labels\":[\"Day\"]}]", queryInstants("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.range({root: n, start: {startTime}, end: {endTime}}) YIELD instant RETURN instant", params), JSONCompareMode.LENIENT);

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

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstMilliInstantIsRequested() throws JSONException {
        long dateInMillis = new DateTime(2014, 4, 5, 13, 56, 22, 123, DateTimeZone.UTC).getMillis();

        Map<String, Object> params = toMap("time", dateInMillis, "resolution", "millisecond", "timezone", "GMT+1");
        assertNoResults("CALL ga.timetree.single({time: {time}, timezone: {timezone}, resolution: {resolution}})", params);

        assertEquals("{\"properties\":{\"value\":123},\"labels\":[\"Millisecond\"]}", querySingleInstant("CALL ga.timetree.single({time: {time}, timezone: {timezone}, resolution: {resolution}, create: true})", params), JSONCompareMode.LENIENT);

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

        assertEquals("{\"properties\":{\"value\":123},\"labels\":[\"Millisecond\"]}", querySingleInstant("CALL ga.timetree.single({time: {time}, timezone: {timezone}, resolution: {resolution}, create: true})", params), JSONCompareMode.LENIENT);
        assertEquals("{\"properties\":{\"value\":123},\"labels\":[\"Millisecond\"]}", querySingleInstant("CALL ga.timetree.single({time: {time}, timezone: {timezone}, resolution: {resolution}})", params), JSONCompareMode.LENIENT);

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
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenTodayIsRequested() throws JSONException {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        assertNoResults("CALL ga.timetree.now({}) YIELD instant RETURN instant", Collections.emptyMap());

        String result = querySingleInstant("CALL ga.timetree.now({create:true}) YIELD instant RETURN instant", Collections.emptyMap());
        assertEquals("{\"properties\":{\"value\":" + now.getDayOfMonth() + "},\"labels\":[\"Day\"]}", result, JSONCompareMode.LENIENT);

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

        assertEquals("{\"properties\":{\"value\":" + now.getDayOfMonth() + "},\"labels\":[\"Day\"]}", querySingleInstant("CALL ga.timetree.now({create:true}) YIELD instant RETURN instant", Collections.emptyMap()), JSONCompareMode.LENIENT);
        assertEquals("{\"properties\":{\"value\":" + now.getDayOfMonth() + "},\"labels\":[\"Day\"]}", querySingleInstant("CALL ga.timetree.now({}) YIELD instant RETURN instant", Collections.emptyMap()), JSONCompareMode.LENIENT);

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

    }

    @Test
    public void trivialTreeShouldBeCreatedWhenTodayIsRequestedWithCustomRoot() throws JSONException {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        assertNoResults("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.now({root:n}) YIELD instant RETURN instant", Collections.emptyMap());
        assertNoResults("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.now({root:n, create:true}) YIELD instant RETURN instant", Collections.emptyMap());

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(label("CustomRoot"));
            tx.success();
        }

        assertNoResults("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.now({root:n}) YIELD instant RETURN instant", Collections.emptyMap());

        String result = querySingleInstant("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.now({root:n, create:true}) YIELD instant RETURN instant", Collections.emptyMap());
        assertEquals("{\"properties\":{\"value\":" + now.getDayOfMonth() + "},\"labels\":[\"Day\"]}", result, JSONCompareMode.LENIENT);

        assertSameGraph(getDatabase(), "CREATE" +
                "(root:CustomRoot)," +
                "(root)-[:FIRST]->(year:Year {value:" + now.getYear() + "})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:" + now.getMonthOfYear() + "})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:" + now.getDayOfMonth() + "})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)");

        assertEquals("{\"properties\":{\"value\":" + now.getDayOfMonth() + "},\"labels\":[\"Day\"]}", querySingleInstant("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.now({root:n, create:true}) YIELD instant RETURN instant", Collections.emptyMap()), JSONCompareMode.LENIENT);
        assertEquals("{\"properties\":{\"value\":" + now.getDayOfMonth() + "},\"labels\":[\"Day\"]}", querySingleInstant("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.now({root:n}) YIELD instant RETURN instant", Collections.emptyMap()), JSONCompareMode.LENIENT);

        assertSameGraph(getDatabase(), "CREATE" +
                "(root:CustomRoot)," +
                "(root)-[:FIRST]->(year:Year {value:" + now.getYear() + "})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:" + now.getMonthOfYear() + "})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:" + now.getDayOfMonth() + "})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)");
    }

    @Test
    public void whenTheRootIsDeletedSubsequentRestApiCallsShouldBeOK() throws JSONException {
        long dateInMillis = dateToMillis(2013, 5, 4);

        Map<String, Object> params = Collections.singletonMap("time", dateInMillis);
        String result = querySingleInstant("CALL ga.timetree.single({time: {time}, create:true})", params);

        assertEquals("{\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", result, JSONCompareMode.LENIENT);

        try (Transaction tx = getDatabase().beginTx()) {
            for (Node node : getDatabase().getAllNodes()) {
                EntityUtils.deleteNodeAndRelationships(node);
            }
            tx.success();
        }

        result = querySingleInstant("CALL ga.timetree.single({time: {time}, create:true})", params);
        assertEquals("{\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", result, JSONCompareMode.LENIENT);

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

        assertEquals("{\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", querySingleInstant("CALL ga.timetree.single({time: {time}, create:true})", params), JSONCompareMode.LENIENT);
        assertEquals("{\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", querySingleInstant("CALL ga.timetree.single({time: {time}})", params), JSONCompareMode.LENIENT);

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
    }

    @Test
    public void whenTheCustomRootIsDeletedSubsequentRestApiCallsShouldThrowNotFoundException() throws JSONException {
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(label("CustomRoot"));
            tx.success();
        }

        long dateInMillis = dateToMillis(2013, 5, 4);
        String result = querySingleInstant("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}, create:true}) YIELD instant RETURN instant", toMap("time", dateInMillis));
        assertEquals("{\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", result, JSONCompareMode.LENIENT);

        try (Transaction tx = getDatabase().beginTx()) {
            for (Node node : getDatabase().getAllNodes()) {
                EntityUtils.deleteNodeAndRelationships(node);
            }
            tx.success();
        }

        assertNoResults("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}, create:true}) YIELD instant RETURN instant", toMap("time", dateInMillis));
        assertNoResults("MATCH (n) WHERE id(n) = 0 CALL ga.timetree.single({root: n, time: {time}}) YIELD instant RETURN instant", toMap("time", dateInMillis));
    }

    @Test
    public void timeZoneShouldWork() throws JSONException {
        //Given
        long dateInMillis = new DateTime(2014, 10, 25, 6, 36, DateTimeZone.UTC).getMillis();

        //When
        String timezone = "America/Los_Angeles";
        String result = querySingleInstant("CALL ga.timetree.single({time: {time}, create:true, resolution:{resolution}, timezone:{timezone}}) YIELD instant RETURN instant", toMap("time", dateInMillis, "resolution", "minute", "timezone", timezone));

        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:2014})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:10})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:24})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)," +
                "(day)-[:CHILD]->(hour:Hour{value:23})," +
                "(day)-[:FIRST]->(hour)," +
                "(day)-[:LAST]->(hour)," +
                "(hour)-[:CHILD]->(minute:Minute{value:36})," +
                "(hour)-[:FIRST]->(minute)," +
                "(hour)-[:LAST]->(minute)"
        );

        assertEquals("{\"properties\":{\"value\":36},\"labels\":[\"Minute\"]}", result, JSONCompareMode.LENIENT);
    }

    @Test
    public void timeZoneShouldWork2() throws JSONException {
        //Given
        long dateInMillis = 1414264162000L;

        //When
        String timezone = "PST";
        String result = querySingleInstant("CALL ga.timetree.single({time: {time}, create:true, resolution:{resolution}, timezone:{timezone}}) YIELD instant RETURN instant", toMap("time", dateInMillis, "resolution", "minute", "timezone", timezone));

        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:2014})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:10})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:25})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)," +
                "(day)-[:CHILD]->(hour:Hour{value:12})," +
                "(day)-[:FIRST]->(hour)," +
                "(day)-[:LAST]->(hour)," +
                "(hour)-[:CHILD]->(minute:Minute{value:9})," +
                "(hour)-[:FIRST]->(minute)," +
                "(hour)-[:LAST]->(minute)"
        );

        assertEquals("{\"properties\":{\"value\":9},\"labels\":[\"Minute\"]}", result, JSONCompareMode.LENIENT);
    }

    @Test
    public void shouldSupportDatesBefore1970() throws JSONException {
        //Given
        long dateInMillis = dateToMillis(1940, 2, 5);

        //When
        String result = querySingleInstant("CALL ga.timetree.single({time: {time}, create:true}) YIELD instant RETURN instant", toMap("time", dateInMillis));

        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value:1940})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:2})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:5})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)");

        assertEquals("{\"properties\":{\"value\":5},\"labels\":[\"Day\"]}", result, JSONCompareMode.LENIENT);
    }

    private long dateToMillis(int year, int month, int day) {
        return dateToDateTime(year, month, day).getMillis();
    }

    private DateTime dateToDateTime(int year, int month, int day) {
        return new DateTime(year, month, day, 0, 0, DateTimeZone.UTC);
    }

    public static Map<String, Object> toMap(Object... objects) {
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < objects.length - 1; i += 2) {
            result.put((String) objects[i], objects[i + 1]);
        }
        return result;
    }

    private void assertNoResults(String query, Map<String, Object> params) {
        Result r = getDatabase().execute(query, params);
        assertFalse(r.hasNext());
    }

    private String querySingleInstant(String query, Map<String, Object> params) {
        Result r = getDatabase().execute(query, params);
        return nodeToJson((Node) r.columnAs("instant").next());
    }

    private String queryInstants(String query, Map<String, Object> params) {
        Result r = getDatabase().execute(query, params);
        System.out.println(r.resultAsString());
        try (Transaction tx = getDatabase().beginTx()) {
            List<LongIdJsonNode> nodes = r.columnAs("instant").stream().map(o -> new LongIdJsonNode((Node) o)).collect(Collectors.toList());
            try {
                return mapper.writeValueAsString(nodes);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String nodeToJson(Node node) {
        String result;
        try (Transaction tx = getDatabase().beginTx()) {
            try {
                result = mapper.writeValueAsString(new LongIdJsonNode(node));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }
}
