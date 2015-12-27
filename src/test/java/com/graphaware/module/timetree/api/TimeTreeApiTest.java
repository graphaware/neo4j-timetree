/*
 * Copyright (c) 2013-2015 GraphAware
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

import com.graphaware.common.util.PropertyContainerUtils;
import com.graphaware.test.integration.GraphAwareApiTest;
import org.apache.http.HttpStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

/**
 * Integration test for {@link com.graphaware.module.timetree.api.TimeTreeApi}.
 */
public class TimeTreeApiTest extends GraphAwareApiTest {

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequested() throws JSONException {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);

        //When
        String result = httpClient.get(getUrl() + "single/" + dateInMillis, HttpStatus.SC_OK);

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

        assertEquals("{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", result, false);
    }

    @Test
    public void consecutiveDaysShouldBeCreatedWhenRequested() throws JSONException {

        //Given
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);

        //When
        String result = httpClient.get(getUrl() + "range/" + startDateInMillis + "/" + endDateInMillis, HttpStatus.SC_OK);

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

        assertEquals("[{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]},{\"id\":4,\"properties\":{\"value\":5},\"labels\":[\"Day\"]},{\"id\":5,\"properties\":{\"value\":6},\"labels\":[\"Day\"]},{\"id\":6,\"properties\":{\"value\":7},\"labels\":[\"Day\"]}]", result, false);
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequestedWithCustomRoot() throws JSONException {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("CustomRoot"));
            tx.success();
        }

        String result = httpClient.get(getUrl() + "0/single/" + dateInMillis, HttpStatus.SC_OK);

        //Then
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

        assertEquals("{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", result, false);
    }

    @Test
    public void consecutiveDaysShouldBeCreatedWhenRequestedWithCustomRoot() throws JSONException {

        //Given
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("CustomRoot"));
            tx.success();
        }

        String result = httpClient.get(getUrl() + "0/range/" + startDateInMillis + "/" + endDateInMillis, HttpStatus.SC_OK);

        //Then
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

        assertEquals("[{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]},{\"id\":4,\"properties\":{\"value\":5},\"labels\":[\"Day\"]},{\"id\":5,\"properties\":{\"value\":6},\"labels\":[\"Day\"]},{\"id\":6,\"properties\":{\"value\":7},\"labels\":[\"Day\"]}]", result, false);
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstMilliInstantIsRequested() throws JSONException {
        //Given
        long dateInMillis = new DateTime(2014, 4, 5, 13, 56, 22, 123, DateTimeZone.UTC).getMillis();

        //When
        String result = httpClient.get(getUrl() + "single/" + dateInMillis + "?resolution=millisecond&timezone=GMT%2B1", HttpStatus.SC_OK);

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

        assertEquals("{\"id\":7,\"properties\":{\"value\":123},\"labels\":[\"Millisecond\"]}", result, false);
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenTodayIsRequested() throws JSONException {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);

        //When
        String result = httpClient.get(getUrl() + "now", HttpStatus.SC_OK);

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

        assertEquals("{\"id\":3,\"properties\":{\"value\":" + now.getDayOfMonth() + "},\"labels\":[\"Day\"]}", result, false);
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenTodayIsRequestedWithCustomRoot() throws JSONException {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("CustomRoot"));
            tx.success();
        }

        String result = httpClient.get(getUrl() + "/0/now", HttpStatus.SC_OK);

        //Then
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

        assertEquals("{\"id\":3,\"properties\":{\"value\":" + now.getDayOfMonth() + "},\"labels\":[\"Day\"]}", result, false);
    }

    @Test
    public void whenTheRootIsDeletedSubsequentRestApiCallsShouldBeOK() throws JSONException {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        String result = httpClient.get(getUrl() + "single/" + dateInMillis, HttpStatus.SC_OK);
        assertEquals("{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", result, false);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            for (Node node : GlobalGraphOperations.at(getDatabase()).getAllNodes()) {
                PropertyContainerUtils.deleteNodeAndRelationships(node);
            }
            tx.success();
        }

        //Then
        result = httpClient.get(getUrl() + "single/" + dateInMillis, HttpStatus.SC_OK);

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

        assertEquals("{\"id\":7,\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", result, false);
    }

    @Test
    public void whenTheCustomRootIsDeletedSubsequentRestApiCallsShouldThrowNotFoundException() throws JSONException {
        //Given
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("CustomRoot"));
            tx.success();
        }

        long dateInMillis = dateToMillis(2013, 5, 4);
        String result = httpClient.get(getUrl() + "0/single/" + dateInMillis, HttpStatus.SC_OK);
        assertEquals("{\"id\":3,\"properties\":{\"value\":4},\"labels\":[\"Day\"]}", result, false);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            for (Node node : GlobalGraphOperations.at(getDatabase()).getAllNodes()) {
                PropertyContainerUtils.deleteNodeAndRelationships(node);
            }
            tx.success();
        }

        //Then
        httpClient.get(getUrl() + "0/single/" + dateInMillis, HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void timeZoneShouldWork() throws JSONException {
        //Given
        long dateInMillis = new DateTime(2014, 10, 25, 6, 36, DateTimeZone.UTC).getMillis();

        //When
        String timezone = "America/Los_Angeles";
        String result = httpClient.get(getUrl() + "single/" + dateInMillis + "?resolution=minute&timezone=" + timezone, HttpStatus.SC_OK);

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

        assertEquals("{\"id\":5,\"properties\":{\"value\":36},\"labels\":[\"Minute\"]}", result, false);
    }

    @Test
    public void timeZoneShouldWork2() throws JSONException {
        //Given
        long dateInMillis = 1414264162000L;

        //When
        String timezone = "PST";
        String result = httpClient.get(getUrl() + "single/" + dateInMillis + "?resolution=minute&timezone=" + timezone, HttpStatus.SC_OK);

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

        assertEquals("{\"id\":5,\"properties\":{\"value\":9},\"labels\":[\"Minute\"]}", result, false);
    }

    @Test
    public void shouldSupportDatesBefore1970() throws JSONException {
        //Given
        long dateInMillis = dateToMillis(1940, 2, 5);

        //When
        String result = httpClient.get(getUrl() + "single/" + dateInMillis, HttpStatus.SC_OK);

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

        assertEquals("{\"id\":3,\"properties\":{\"value\":5},\"labels\":[\"Day\"]}", result, false);
    }

    private long dateToMillis(int year, int month, int day) {
        return dateToDateTime(year, month, day).getMillis();
    }

    private DateTime dateToDateTime(int year, int month, int day) {
        return new DateTime(year, month, day, 0, 0, DateTimeZone.UTC);
    }

    private String getUrl() {
        return baseUrl() + "/timetree/";
    }
}
