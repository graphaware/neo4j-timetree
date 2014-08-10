/*
 * Copyright (c) 2013 GraphAware
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
import com.graphaware.test.integration.GraphAwareApiTest;
import org.apache.http.HttpStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static com.graphaware.test.util.TestUtils.get;
import static com.graphaware.test.util.TestUtils.post;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for {@link com.graphaware.module.timetree.api.TimeTreeApi}.
 */
public class TimeTreeApiTest extends GraphAwareApiTest {

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequested() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);

        //When
        String result = get(getUrl() + "single/" + dateInMillis, HttpStatus.SC_OK);

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

        assertEquals("3", result);
    }

    @Test
    public void consecutiveDaysShouldBeCreatedWhenRequested() {

        //Given
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);

        //When
        String result = get(getUrl() + "range/" + startDateInMillis + "/" + endDateInMillis, HttpStatus.SC_OK);

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

        assertEquals("[3,4,5,6]", result);
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequestedWithCustomRoot() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("CustomRoot"));
            tx.success();
        }

        String result = get(getUrl() + "0/single/" + dateInMillis, HttpStatus.SC_OK);

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

        assertEquals("3", result);
    }

    @Test
    public void consecutiveDaysShouldBeCreatedWhenRequestedWithCustomRoot() {

        //Given
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("CustomRoot"));
            tx.success();
        }

        String result = get(getUrl() + "0/range/" + startDateInMillis + "/" + endDateInMillis, HttpStatus.SC_OK);

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

        assertEquals("[3,4,5,6]", result);
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstMilliInstantIsRequested() {
        //Given
        long dateInMillis = new DateTime(2014, 4, 5, 13, 56, 22, 123, DateTimeZone.UTC).getMillis();

        //When
        String result = get(getUrl() + "single/" + dateInMillis + "?resolution=millisecond&timezone=GMT%2B1", HttpStatus.SC_OK);

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

        assertEquals("7", result);
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenTodayIsRequested() {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);

        //When
        String result = get(getUrl() + "now", HttpStatus.SC_OK);

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

        assertEquals("3", result);
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenTodayIsRequestedWithCustomRoot() {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("CustomRoot"));
            tx.success();
        }

        String result = get(getUrl() + "/0/now", HttpStatus.SC_OK);

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

        assertEquals("3", result);
    }

    @Test
    public void eventAndTimeInstantShouldBeCreatedWhenEventIsAttached() {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);
        TimeInstant timeInstant = new TimeInstant();
        timeInstant.setResolution(Resolution.DAY);
        timeInstant.setTimezone(DateTimeZone.UTC);

        Node event;
        long eventId;
        try (Transaction tx = getDatabase().beginTx()) {
            event = getDatabase().createNode();
            event.setProperty("name", "eventA");
            eventId = event.getId();
            tx.success();
        }


        //When
        String eventJson = "{" +
                "        \"eventNodeId\": " + eventId + "," +
                "        \"eventRelationshipType\": \"HAS_EVENT\"," +
                "        \"eventRelationshipDirection\": \"INCOMING\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";
        System.out.println("eventJson = " + eventJson);
        post(getUrl() + "single/event", eventJson, HttpStatus.SC_OK);

        String getResult = get(getUrl() + "single/" + timeInstant.getTime() + "/events", HttpStatus.SC_OK);

        System.out.println("getResult = " + getResult);
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
                "(day)-[:HAS_EVENT]->(event {name:'eventA'})");

        assertTrue(getResult.contains("{\"eventNodeId\":0,\"eventRelationshipType\":\"HAS_EVENT\",\"timezone\":\"UTC\",\"resolution\":\"DAY\",\"time\":" + timeInstant.getTime() + "}"));
    }


    @Test
    public void eventAndTimeInstantAtCustomRootShouldBeCreatedWhenEventIsAttached() {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);
        TimeInstant timeInstant = new TimeInstant();
        timeInstant.setResolution(Resolution.DAY);
        timeInstant.setTimezone(DateTimeZone.UTC);

        Node event;
        long eventId;
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("CustomRoot"));
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            event = getDatabase().createNode();
            event.setProperty("name", "eventA");
            eventId = event.getId();
            tx.success();
        }


        //When
        String eventJson = "{" +
                "        \"eventNodeId\": " + eventId + "," +
                "        \"eventRelationshipType\": \"HAS_EVENT\"," +
                "        \"eventRelationshipDirection\": \"INCOMING\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";
        System.out.println("eventJson = " + eventJson);
        post(getUrl() + "0/single/event", eventJson, HttpStatus.SC_OK);

        String getResult = get(getUrl() + "0/single/" + timeInstant.getTime() + "/events", HttpStatus.SC_OK);

        System.out.println("getResult = " + getResult);
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
                "(month)-[:LAST]->(day)," +
                "(day)-[:HAS_EVENT]->(event {name:'eventA'})");

        assertTrue(getResult.contains("{\"eventNodeId\":1,\"eventRelationshipType\":\"HAS_EVENT\",\"timezone\":\"UTC\",\"resolution\":\"DAY\",\"time\":" + timeInstant.getTime() + "}"));
    }


    @Test
    public void multipleEventsAndTimeInstantsShouldBeCreatedWhenEventsAreAttached() {
        //Given
        TimeInstant timeInstant1 = new TimeInstant(dateToMillis(2012, 11, 1));
        timeInstant1.setResolution(Resolution.DAY);
        timeInstant1.setTimezone(DateTimeZone.UTC);

        TimeInstant timeInstant2 = new TimeInstant(dateToMillis(2012, 11, 3));
        timeInstant2.setResolution(Resolution.DAY);
        timeInstant2.setTimezone(DateTimeZone.UTC);
        Node event1, event2;

        long eventId1, eventId2;
        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            eventId1 = event1.getId();

            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            eventId2 = event2.getId();
            tx.success();
        }


        //When
        String eventJson1 = "{" +
                "        \"eventNodeId\": " + eventId1 + "," +
                "        \"eventRelationshipType\": \"HAS_EVENT\"," +
                "        \"eventRelationshipDirection\": \"INCOMING\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant1.getTime() +
                "    }";
        String eventJson2 = "{" +
                "        \"eventNodeId\": " + eventId2 + "," +
                "        \"eventRelationshipType\": \"HAS_EVENT\"," +
                "        \"eventRelationshipDirection\": \"INCOMING\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant2.getTime() +
                "    }";

        post(getUrl() + "single/event", eventJson1, HttpStatus.SC_OK);
        post(getUrl() + "single/event", eventJson2, HttpStatus.SC_OK);


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
                "(day)-[:HAS_EVENT]->(event1 {name:'eventA'})," +
                "(day2)-[:HAS_EVENT]->(event2 {name:'eventB'})");

        String getResult = get(getUrl() + "range/" + timeInstant1.getTime() + "/" + timeInstant2.getTime() + "/events", HttpStatus.SC_OK);
        assertTrue(getResult.contains("{\"eventNodeId\":0,\"eventRelationshipType\":\"HAS_EVENT\",\"timezone\":\"UTC\",\"resolution\":\"DAY\",\"time\":" + timeInstant1.getTime() + "},{\"eventNodeId\":1,\"eventRelationshipType\":\"HAS_EVENT\",\"timezone\":\"UTC\",\"resolution\":\"DAY\",\"time\":" + timeInstant2.getTime() + "}"));
    }


    @Test
    public void multipleEventsAndTimeInstantsForCustomRootShouldBeCreatedWhenEventsAreAttached() {
        //Given
        TimeInstant timeInstant1 = new TimeInstant(dateToMillis(2012, 11, 1));
        timeInstant1.setResolution(Resolution.DAY);
        timeInstant1.setTimezone(DateTimeZone.UTC);

        TimeInstant timeInstant2 = new TimeInstant(dateToMillis(2012, 11, 3));
        timeInstant2.setResolution(Resolution.DAY);
        timeInstant2.setTimezone(DateTimeZone.UTC);
        Node event1, event2;

        long eventId1, eventId2;
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("CustomRoot"));
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            eventId1 = event1.getId();

            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            eventId2 = event2.getId();
            tx.success();
        }


        //When
        String eventJson1 = "{" +
                "        \"eventNodeId\": " + eventId1 + "," +
                "        \"eventRelationshipType\": \"HAS_EVENT\"," +
                "        \"eventRelationshipDirection\": \"INCOMING\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant1.getTime() +
                "    }";
        String eventJson2 = "{" +
                "        \"eventNodeId\": " + eventId2 + "," +
                "        \"eventRelationshipType\": \"HAS_EVENT\"," +
                "        \"eventRelationshipDirection\": \"INCOMING\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant2.getTime() +
                "    }";

        post(getUrl() + "0/single/event", eventJson1, HttpStatus.SC_OK);
        post(getUrl() + "0/single/event", eventJson2, HttpStatus.SC_OK);


        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:CustomRoot)," +
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
                "(day)-[:HAS_EVENT]->(event1 {name:'eventA'})," +
                "(day2)-[:HAS_EVENT]->(event2 {name:'eventB'})");

        String getResult = get(getUrl() + "0/range/" + timeInstant1.getTime() + "/" + timeInstant2.getTime() + "/events", HttpStatus.SC_OK);
        assertTrue(getResult.contains("{\"eventNodeId\":1,\"eventRelationshipType\":\"HAS_EVENT\",\"timezone\":\"UTC\",\"resolution\":\"DAY\",\"time\":" + timeInstant1.getTime() + "},{\"eventNodeId\":2,\"eventRelationshipType\":\"HAS_EVENT\",\"timezone\":\"UTC\",\"resolution\":\"DAY\",\"time\":" + timeInstant2.getTime() + "}"));
    }


    @Test
    public void whenTheRootIsDeletedSubsequentRestApiCallsShouldThrowNotFoundException() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        String result = get(getUrl() + "single/" + dateInMillis, HttpStatus.SC_OK);
        assertEquals("3", result);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            for (Node node : GlobalGraphOperations.at(getDatabase()).getAllNodes()) {
                PropertyContainerUtils.deleteNodeAndRelationships(node);
            }
            tx.success();
        }

        //Then
        get(getUrl() + "single/" + dateInMillis, HttpStatus.SC_NOT_FOUND);

        //Caches invalidated, try again
        result = get(getUrl() + "single/" + dateInMillis, HttpStatus.SC_OK);

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

        assertEquals("7", result);
    }

    @Test
    public void whenTheCustomRootIsDeletedSubsequentRestApiCallsShouldThrowNotFoundException() {
        //Given
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("CustomRoot"));
            tx.success();
        }

        long dateInMillis = dateToMillis(2013, 5, 4);
        String result = get(getUrl() + "0/single/" + dateInMillis, HttpStatus.SC_OK);
        assertEquals("3", result);

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            for (Node node : GlobalGraphOperations.at(getDatabase()).getAllNodes()) {
                PropertyContainerUtils.deleteNodeAndRelationships(node);
            }
            tx.success();
        }

        //Then
        get(getUrl() + "0/single/" + dateInMillis, HttpStatus.SC_NOT_FOUND);
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
