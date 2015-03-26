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

package com.graphaware.module.timetree.api;

import com.graphaware.module.timetree.domain.Resolution;
import com.graphaware.module.timetree.domain.TimeInstant;
import com.graphaware.test.integration.GraphAwareApiTest;
import com.graphaware.test.unit.GraphUnit;
import org.apache.http.HttpStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.IOException;
import java.util.Collections;

import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static com.graphaware.test.util.TestUtils.get;
import static com.graphaware.test.util.TestUtils.post;
import static org.junit.Assert.assertEquals;

/**
 * Integration test for {@link TimeTreeApi}.
 */
public class TimedEventsApiTest extends GraphAwareApiTest {

    @Test
    public void shouldComplainWhenInputIllegal() throws IOException {

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode();
            tx.success();
        }

        String eventJson = "{" +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + System.currentTimeMillis() +
                "    }";

        post(getUrl() + "single/event", eventJson, HttpStatus.SC_BAD_REQUEST);

        eventJson = "{" +
                "        \"nodeId\":0," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"time\": " + System.currentTimeMillis() +
                "    }";

        post(getUrl() + "single/event", eventJson, HttpStatus.SC_CREATED);

        eventJson = "{" +
                "        \"nodeId\":100," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"time\": " + System.currentTimeMillis() +
                "    }";

        post(getUrl() + "single/event", eventJson, HttpStatus.SC_NOT_FOUND);

        eventJson = "{" +
                "        \"nodeId\":0," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"" +
                "    }";

        post(getUrl() + "single/event", eventJson, HttpStatus.SC_BAD_REQUEST);

        eventJson = "{" +
                "        \"nodeId\":0," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + System.currentTimeMillis() +
                "    }";

        post(getUrl() + "single/event", eventJson, HttpStatus.SC_BAD_REQUEST);

        eventJson = "{" +
                "        \"nodeId\":0," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"ILLEGAL\"," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"time\": " + System.currentTimeMillis() +
                "    }";

        post(getUrl() + "single/event", eventJson, HttpStatus.SC_BAD_REQUEST);

        eventJson = "{" +
                "        \"nodeId\":0," +
                "        \"timezone\": \"ILLEGAL\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"time\": " + (System.currentTimeMillis() - 1000 * 3600 * 24 * 2) + //two days less, we already have an event for this day
                "    }";

        post(getUrl() + "single/event", eventJson, HttpStatus.SC_CREATED); //with default timezone

        get(getUrl() + "range/2/1/events", HttpStatus.SC_BAD_REQUEST);
    }

    @Test
    public void eventAndTimeInstantShouldBeCreatedWhenEventIsAttached() throws IOException {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);
        TimeInstant timeInstant = TimeInstant
                .now()
                .with(DateTimeZone.UTC)
                .with(Resolution.DAY);

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
                "        \"nodeId\": " + eventId + "," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";

        post(getUrl() + "single/event", eventJson, HttpStatus.SC_CREATED);

        String getResult = get(getUrl() + "single/" + timeInstant.getTime() + "/events?relationshipType=AT_TIME", HttpStatus.SC_OK);

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

        assertEquals("[{\"nodeId\":0,\"relationshipType\":\"AT_TIME\"}]", getResult);
    }

    @Test
    public void eventShouldOnlyBeAttachedOnce() throws IOException {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);
        TimeInstant timeInstant = TimeInstant
                .now()
                .with(DateTimeZone.UTC)
                .with(Resolution.DAY);

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
                "        \"nodeId\": " + eventId + "," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";

        post(getUrl() + "single/event", eventJson, HttpStatus.SC_CREATED);
        post(getUrl() + "single/event", eventJson, HttpStatus.SC_OK);
        post(getUrl() + "single/event", eventJson, HttpStatus.SC_OK);

        String getResult = get(getUrl() + "single/" + timeInstant.getTime() + "/events?relationshipType=AT_TIME", HttpStatus.SC_OK);

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

        assertEquals("[{\"nodeId\":0,\"relationshipType\":\"AT_TIME\"}]", getResult);
    }


    @Test
    public void eventAndTimeInstantAtCustomRootShouldBeCreatedWhenEventIsAttached() {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);
        TimeInstant timeInstant = TimeInstant
                .now()
                .with(DateTimeZone.UTC)
                .with(Resolution.DAY);

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
                "        \"nodeId\": " + eventId + "," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";

        post(getUrl() + "0/single/event", eventJson, HttpStatus.SC_CREATED);

        String getResult = get(getUrl() + "0/single/" + timeInstant.getTime() + "/events", HttpStatus.SC_OK);

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
                "(day)<-[:AT_TIME]-(event {name:'eventA'})");

        assertEquals("[{\"nodeId\":1,\"relationshipType\":\"AT_TIME\"}]", getResult);
    }


    @Test
    public void multipleEventsAndTimeInstantsShouldBeCreatedWhenEventsAreAttached() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1)).with(Resolution.DAY).with(DateTimeZone.UTC);
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 3)).with(Resolution.DAY).with(DateTimeZone.UTC);

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
                "        \"nodeId\": " + eventId1 + "," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant1.getTime() +
                "    }";
        String eventJson2 = "{" +
                "        \"nodeId\": " + eventId2 + "," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant2.getTime() +
                "    }";

        post(getUrl() + "single/event", eventJson1, HttpStatus.SC_CREATED);
        post(getUrl() + "single/event", eventJson2, HttpStatus.SC_CREATED);


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

        String getResult = get(getUrl() + "range/" + timeInstant1.getTime() + "/" + timeInstant2.getTime() + "/events", HttpStatus.SC_OK);

        assertEquals("[{\"nodeId\":0,\"relationshipType\":\"AT_TIME\"}," +
                "{\"nodeId\":1,\"relationshipType\":\"AT_TIME\"}]", getResult);
    }


    @Test
    public void multipleEventsAndTimeInstantsForCustomRootShouldBeCreatedWhenEventsAreAttached() {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1)).with(Resolution.DAY).with(DateTimeZone.UTC);
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 3)).with(Resolution.DAY).with(DateTimeZone.UTC);

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
                "        \"nodeId\": " + eventId1 + "," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant1.getTime() +
                "    }";
        String eventJson2 = "{" +
                "        \"nodeId\": " + eventId2 + "," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant2.getTime() +
                "    }";

        post(getUrl() + "0/single/event", eventJson1, HttpStatus.SC_CREATED);
        post(getUrl() + "0/single/event", eventJson2, HttpStatus.SC_CREATED);


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
                "(day)<-[:AT_TIME]-(event1 {name:'eventA'})," +
                "(day2)<-[:AT_TIME]-(event2 {name:'eventB'})");

        String getResult = get(getUrl() + "0/range/" + timeInstant1.getTime() + "/" + timeInstant2.getTime() + "/events", HttpStatus.SC_OK);

        assertEquals("[{\"nodeId\":1,\"relationshipType\":\"AT_TIME\"}," +
                "{\"nodeId\":2,\"relationshipType\":\"AT_TIME\"}]", getResult);
    }

    @Test //issue https://github.com/graphaware/neo4j-timetree/issues/12
    public void shouldBeAbleToAttachEventsInARunningTx() {
        post(baseNeoUrl() + "/db/data/transaction", "{\n" +
                "  \"statements\" : [ {\n" +
                "    \"statement\" : \"CREATE (e:Email {props}) RETURN id(e)\",\n" +
                "    \"parameters\" : {\n" +
                "      \"props\" : {\n" +
                "        \"subject\" : \"Neo4j\"\n" +
                "      }\n" +
                "    }\n" +
                "  } ]\n" +
                "}", HttpStatus.SC_CREATED);

        //When
        String eventJson1 = "{" +
                "        \"nodeId\": " + 0 + "," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": 123343242132" +
                "    }";

        //we should parse out the tx id, but we know it's 1 on a new database:

        post(getUrl() + "single/event", eventJson1, Collections.singletonMap("_GA_TX_ID","1"), HttpStatus.SC_CREATED);

        post(baseNeoUrl() + "/db/data/transaction/1/commit", HttpStatus.SC_OK);

        GraphUnit.assertSameGraph(getDatabase(), "CREATE " +
                "(e:Email {subject: 'Neo4j'})," +
                "(r:TimeTreeRoot)," +
                "(y:Year {value: 1973})," +
                "(m:Month {value: 11})," +
                "(d:Day {value: 28})," +
                "(r)-[:CHILD]->(y)," +
                "(r)-[:FIRST]->(y)," +
                "(r)-[:LAST]->(y)," +
                "(y)-[:CHILD]->(m)," +
                "(y)-[:FIRST]->(m)," +
                "(y)-[:LAST]->(m)," +
                "(m)-[:CHILD]->(d)," +
                "(m)-[:FIRST]->(d)," +
                "(m)-[:LAST]->(d)," +
                "(e)-[:AT_TIME]->(d)");
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
