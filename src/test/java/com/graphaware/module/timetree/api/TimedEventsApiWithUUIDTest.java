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

package com.graphaware.module.timetree.api;

import com.graphaware.common.policy.inclusion.InclusionPolicies;
import com.graphaware.common.policy.inclusion.NodePropertyInclusionPolicy;
import com.graphaware.module.timetree.domain.Resolution;
import com.graphaware.module.timetree.domain.TimeInstant;
import com.graphaware.test.integration.GraphAwareIntegrationTest;
import org.apache.http.HttpStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.neo4j.graphdb.Label.label;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

/**
 * Integration test for {@link TimeTreeApi}.
 */
public class TimedEventsApiWithUUIDTest extends GraphAwareIntegrationTest {

    private final AtomicInteger counter = new AtomicInteger(0);
    private InclusionPolicies ignoreUuid = InclusionPolicies.all().with(
            new NodePropertyInclusionPolicy() {
                @Override
                public boolean include(String s, Node node) {
                    return !s.equals("uuid");
                }
            });

    @Override
    protected void populateDatabase(GraphDatabaseService database) {
        super.populateDatabase(database);

        counter.set(0);
        database.registerTransactionEventHandler(new TransactionEventHandler.Adapter<Void>() {
            @Override
            public Void beforeCommit(TransactionData data) throws Exception {
                for (Node created : data.createdNodes()) {
                    created.setProperty("uuid", "test-uuid-" + counter.incrementAndGet());
                }

                return null;
            }
        });
    }

    @Test
    public void eventAndTimeInstantShouldBeCreatedWhenEventIsAttached() throws IOException, JSONException {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);
        TimeInstant timeInstant = TimeInstant
                .now()
                .with(DateTimeZone.UTC)
                .with(Resolution.DAY);

        Node event1, event2, event3;
        long eventId1, eventId2, eventId3;
        try (Transaction tx = getDatabase().beginTx()) {
            event1 = getDatabase().createNode();
            event1.setProperty("name", "eventA");
            eventId1 = event1.getId();
            tx.success();

            event2 = getDatabase().createNode();
            event2.setProperty("name", "eventB");
            eventId2 = event2.getId();
            tx.success();

            event3 = getDatabase().createNode();
            event3.setProperty("name", "eventC");
            eventId3 = event3.getId();
            tx.success();
        }


        //When
        String eventJson1 = "{" +
                "        \"node\": {\"id\":" + eventId1 + "}," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";

        String eventJson2 = "{" +
                "        \"node\": {\"id\":" + eventId2 + "}," +
                "        \"relationshipType\": \"AT_OTHER_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";


        String eventJson3 = "{" +
                "        \"node\": {\"id\":" + eventId3 + "}," +
                "        \"relationshipType\": \"AT_BAD_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";

        httpClient.post(getUrl() + "single/event", eventJson1, HttpStatus.SC_CREATED);
        httpClient.post(getUrl() + "single/event", eventJson2, HttpStatus.SC_CREATED);
        String postResult = httpClient.post(getUrl() + "single/event", eventJson3, HttpStatus.SC_CREATED);

        String getResult = httpClient.get(getUrl() + "single/" + timeInstant.getTime() + "/events?relationshipTypes=AT_TIME", HttpStatus.SC_OK);
        String getMultipleResult = httpClient.get(getUrl() + "single/" + timeInstant.getTime() + "/events?relationshipTypes=AT_TIME,AT_OTHER_TIME", HttpStatus.SC_OK);
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
                "(day)<-[:AT_TIME]-(event {name:'eventA'})," +
                "(day)<-[:AT_OTHER_TIME]-(event2 {name:'eventB'})," +
                "(day)<-[:AT_BAD_TIME]-(event3 {name:'eventC'})", ignoreUuid);

        assertEquals("{\"id\":2,\"properties\":{\"name\":\"eventC\",\"uuid\":\"test-uuid-3\"},\"labels\":[]}", postResult, false);
        assertEquals("[{\"node\":{\"id\":0,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-1\"},\"labels\":[]},\"relationshipType\":\"AT_TIME\",\"direction\":\"INCOMING\"}]", getResult, false);
        assertEquals("[{\"node\":{\"id\":1,\"properties\":{\"name\":\"eventB\",\"uuid\":\"test-uuid-2\"},\"labels\":[]},\"relationshipType\":\"AT_OTHER_TIME\",\"direction\":\"INCOMING\"}," +
                "{\"node\":{\"id\":0,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-1\"},\"labels\":[]},\"relationshipType\":\"AT_TIME\",\"direction\":\"INCOMING\"}]", getMultipleResult, false);
    }

    @Test
    public void eventAndTimeInstantShouldBeCreatedWhenEventIsCreatedFromScratch() throws IOException, JSONException {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);
        TimeInstant timeInstant = TimeInstant
                .now()
                .with(DateTimeZone.UTC)
                .with(Resolution.DAY);

        //When
        String eventJson1 = "{" +
                "        \"node\": {\"labels\":[\"Event\",\"Email\"], \"properties\":{\"name\":\"eventA\"}}," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";

        String eventJson2 = "{" +
                "        \"node\": {\"labels\":[\"Event\"], \"properties\":{\"name\":\"eventB\"}}," +
                "        \"relationshipType\": \"AT_OTHER_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";


        String eventJson3 = "{" +
                "        \"node\": {\"labels\":[], \"properties\":{\"name\":\"eventC\"}}," +
                "        \"relationshipType\": \"AT_BAD_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";

        String eventJson4 = "{" +
                "        \"node\": {\"properties\":{\"name\":\"eventD\",\"value\":1}}," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";

        String postResult = httpClient.post(getUrl() + "single/event", eventJson1, HttpStatus.SC_CREATED);
        httpClient.post(getUrl() + "single/event", eventJson2, HttpStatus.SC_CREATED);
        httpClient.post(getUrl() + "single/event", eventJson3, HttpStatus.SC_CREATED);
        httpClient.post(getUrl() + "single/event", eventJson4, HttpStatus.SC_CREATED);

        String getResult = httpClient.get(getUrl() + "single/" + timeInstant.getTime() + "/events", HttpStatus.SC_OK);

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
                "(day)<-[:AT_TIME]-(event:Event:Email {name:'eventA'})," +
                "(day)<-[:AT_TIME]-(event4 {name:'eventD',value:1})," +
                "(day)<-[:AT_OTHER_TIME]-(event2:Event {name:'eventB'})," +
                "(day)<-[:AT_BAD_TIME]-(event3 {name:'eventC'})", ignoreUuid);

        assertEquals("{\"id\":0,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-1\"},\"labels\":[\"Event\",\"Email\"]}", postResult, false);

        assertEquals("[{\"node\":{\"id\":7,\"properties\":{\"name\":\"eventD\",\"value\":1,\"uuid\":\"test-uuid-8\"},\"labels\":[]},\"relationshipType\":\"AT_TIME\",\"direction\":\"INCOMING\"}," +
                "{\"node\":{\"id\":6,\"properties\":{\"name\":\"eventC\",\"uuid\":\"test-uuid-7\"},\"labels\":[]},\"relationshipType\":\"AT_BAD_TIME\",\"direction\":\"INCOMING\"}," +
                "{\"node\":{\"id\":5,\"properties\":{\"name\":\"eventB\",\"uuid\":\"test-uuid-6\"},\"labels\":[\"Event\"]},\"relationshipType\":\"AT_OTHER_TIME\",\"direction\":\"INCOMING\"}," +
                "{\"node\":{\"id\":0,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-1\"},\"labels\":[\"Event\",\"Email\"]},\"relationshipType\":\"AT_TIME\",\"direction\":\"INCOMING\"}]", getResult, false);
    }

    @Test
    public void eventShouldOnlyBeAttachedOnce() throws IOException, JSONException {
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
                "        \"node\": {\"id\":" + eventId + "}," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";

        String postResult1 = httpClient.post(getUrl() + "single/event", eventJson, HttpStatus.SC_CREATED);
        String postResult2 = httpClient.post(getUrl() + "single/event", eventJson, HttpStatus.SC_OK);
        httpClient.post(getUrl() + "single/event", eventJson, HttpStatus.SC_OK);

        String getResult = httpClient.get(getUrl() + "single/" + timeInstant.getTime() + "/events?relationshipType=AT_TIME", HttpStatus.SC_OK);

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
                "(day)<-[:AT_TIME]-(event {name:'eventA'})", ignoreUuid);

        assertEquals("{\"id\":0,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-1\"},\"labels\":[]}", postResult1, false);
        assertEquals("{\"id\":0,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-1\"},\"labels\":[]}", postResult2, false);
        assertEquals("[{\"node\":{\"id\":0,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-1\"},\"labels\":[]},\"relationshipType\":\"AT_TIME\",\"direction\":\"INCOMING\"}]", getResult, false);
    }


    @Test
    public void eventAndTimeInstantAtCustomRootShouldBeCreatedWhenEventIsAttached() throws JSONException {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);
        TimeInstant timeInstant = TimeInstant
                .now()
                .with(DateTimeZone.UTC)
                .with(Resolution.DAY);

        Node event;
        long eventId;
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(label("CustomRoot"));
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            event = getDatabase().createNode(label("Event"));
            event.setProperty("name", "eventA");
            eventId = event.getId();
            tx.success();
        }

        //When
        String eventJson = "{" +
                "        \"node\": {\"id\":" + eventId + "}," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";

        String postResult1 = httpClient.post(getUrl() + "0/single/event", eventJson, HttpStatus.SC_CREATED);
        String postResult2 = httpClient.post(getUrl() + "0/single/event", eventJson, HttpStatus.SC_OK);

        String getResult = httpClient.get(getUrl() + "0/single/" + timeInstant.getTime() + "/events", HttpStatus.SC_OK);

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
                "(day)<-[:AT_TIME]-(event:Event {name:'eventA'})", ignoreUuid);

        assertEquals("{\"id\":1,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-2\"},\"labels\":[\"Event\"]}", postResult1, false);
        assertEquals("{\"id\":1,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-2\"},\"labels\":[\"Event\"]}", postResult2, false);
        assertEquals("[{\"node\":{\"id\":1,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-2\"},\"labels\":[\"Event\"]},\"relationshipType\":\"AT_TIME\",\"direction\":\"INCOMING\"}]", getResult, false);
    }

    @Test
    public void eventAndTimeInstantAtCustomRootShouldBeCreatedWhenEventIsCreatedFromScratch() throws JSONException {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);
        TimeInstant timeInstant = TimeInstant
                .now()
                .with(DateTimeZone.UTC)
                .with(Resolution.DAY);

        Node event;
        long eventId;
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(label("CustomRoot"));
            tx.success();
        }

        //When
        String eventJson = "{" +
                "        \"node\": {\"labels\":[\"Event\"], \"properties\":{\"name\":\"eventA\"}}," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant.getTime() +
                "    }";

        String postResult = httpClient.post(getUrl() + "0/single/event", eventJson, HttpStatus.SC_CREATED);

        String getResult = httpClient.get(getUrl() + "0/single/" + timeInstant.getTime() + "/events", HttpStatus.SC_OK);

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
                "(day)<-[:AT_TIME]-(event:Event {name:'eventA'})", ignoreUuid);

        assertEquals("{\"id\":1,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-2\"},\"labels\":[\"Event\"]}", postResult, false);
        assertEquals("[{\"node\":{\"id\":1,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-2\"},\"labels\":[\"Event\"]},\"relationshipType\":\"AT_TIME\",\"direction\":\"INCOMING\"}]", getResult, false);
    }

    @Test
    public void multipleEventsAndTimeInstantsShouldBeCreatedWhenEventsAreAttached() throws JSONException {
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
                "        \"node\": {\"id\":" + eventId1 + "}," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant1.getTime() +
                "    }";
        String eventJson2 = "{" +
                "        \"node\": {\"id\":" + eventId2 + "}," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant2.getTime() +
                "    }";

        String postResult1 = httpClient.post(getUrl() + "single/event", eventJson1, HttpStatus.SC_CREATED);
        String postResult2 = httpClient.post(getUrl() + "single/event", eventJson2, HttpStatus.SC_CREATED);


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
                "(day2)<-[:AT_TIME]-(event2 {name:'eventB'})", ignoreUuid);

        String getResult = httpClient.get(getUrl() + "range/" + timeInstant1.getTime() + "/" + timeInstant2.getTime() + "/events", HttpStatus.SC_OK);

        assertEquals("{\"id\":0,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-1\"},\"labels\":[]}", postResult1, false);
        assertEquals("{\"id\":1,\"properties\":{\"name\":\"eventB\",\"uuid\":\"test-uuid-2\"},\"labels\":[]}", postResult2, false);
        assertEquals("[{\"node\":{\"id\":0,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-1\"},\"labels\":[]},\"relationshipType\":\"AT_TIME\",\"direction\":\"INCOMING\"}," +
                "{\"node\":{\"id\":1,\"properties\":{\"name\":\"eventB\",\"uuid\":\"test-uuid-2\"},\"labels\":[]},\"relationshipType\":\"AT_TIME\",\"direction\":\"INCOMING\"}]", getResult, false);
    }

    @Test
    public void multipleEventsAndTimeInstantsForCustomRootShouldBeCreatedWhenEventsAreAttached() throws JSONException {
        //Given
        TimeInstant timeInstant1 = TimeInstant.instant(dateToMillis(2012, 11, 1)).with(Resolution.DAY).with(DateTimeZone.UTC);
        TimeInstant timeInstant2 = TimeInstant.instant(dateToMillis(2012, 11, 3)).with(Resolution.DAY).with(DateTimeZone.UTC);

        Node event1, event2;

        long eventId1, eventId2;
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(label("CustomRoot"));
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
                "        \"node\": {\"id\":" + eventId1 + "}," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant1.getTime() +
                "    }";
        String eventJson2 = "{" +
                "        \"node\": {\"id\":" + eventId2 + "}," +
                "        \"relationshipType\": \"AT_TIME\"," +
                "        \"timezone\": \"UTC\"," +
                "        \"resolution\": \"DAY\"," +
                "        \"time\": " + timeInstant2.getTime() +
                "    }";

        String postResult1 = httpClient.post(getUrl() + "0/single/event", eventJson1, HttpStatus.SC_CREATED);
        String postResult2 = httpClient.post(getUrl() + "0/single/event", eventJson2, HttpStatus.SC_CREATED);


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
                "(day2)<-[:AT_TIME]-(event2 {name:'eventB'})", ignoreUuid);

        String getResult = httpClient.get(getUrl() + "0/range/" + timeInstant1.getTime() + "/" + timeInstant2.getTime() + "/events", HttpStatus.SC_OK);

        assertEquals("{\"id\":1,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-2\"},\"labels\":[]}", postResult1, false);
        assertEquals("{\"id\":2,\"properties\":{\"name\":\"eventB\",\"uuid\":\"test-uuid-3\"},\"labels\":[]}", postResult2, false);
        assertEquals("[{\"node\":{\"id\":1,\"properties\":{\"name\":\"eventA\",\"uuid\":\"test-uuid-2\"},\"labels\":[]},\"relationshipType\":\"AT_TIME\",\"direction\":\"INCOMING\"}," +
                "{\"node\":{\"id\":2,\"properties\":{\"name\":\"eventB\",\"uuid\":\"test-uuid-3\"},\"labels\":[]},\"relationshipType\":\"AT_TIME\",\"direction\":\"INCOMING\"}]", getResult, false);
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
