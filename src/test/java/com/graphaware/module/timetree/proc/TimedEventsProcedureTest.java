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

import com.graphaware.module.timetree.domain.Resolution;
import com.graphaware.module.timetree.domain.TimeInstant;
import com.graphaware.test.integration.GraphAwareIntegrationTest;
import org.apache.http.HttpStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import org.junit.Test;
import org.neo4j.graphdb.*;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.*;

/**
 * Procedure test for {@link com.graphaware.module.timetree.proc.TimeTreeProcedure}.
 */
public class TimedEventsProcedureTest extends GraphAwareIntegrationTest {

    private static final String EMAIL = "Email";
    private static final String TIME_PROPERTY = "time";
    private static final String DEFAULT_REL_TYPE = "SENT_ON";

    @Override
    protected String configFile() {
        return "neo4j-timetree.properties";
    }

    @Test
    public void testGetOrCreateInstant() throws JSONException {
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
        
        Map<String, Object> params = new HashMap<>();
        params.put("time", timeInstant.getTime());
        try( Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.events.single({time: {time}}) YIELD node, relationshipType, direction", params);
            int count = 0;
            while (result.hasNext()) {
                Map<String, Object> next = result.next();
                assertNotNull(next.get("node"));
                assertTrue(
                        "AT_TIME".equals(next.get("relationshipType")) ||
                        "AT_BAD_TIME".equals(next.get("relationshipType")) ||
                        "AT_OTHER_TIME".equals(next.get("relationshipType"))
                );
                assertEquals("INCOMING", next.get("direction"));
                count++;
            }
            assertEquals(4, count);
            tx.success();
        }
        
    }

    @Test
    public void testEventsAutoAttachedCanBeRetrievedViaProcedure() {
        long time = dateToMillis(2015, 1, 3, 16);
        createEvent(time);
        int i = 0;
        try (Transaction tx = getDatabase().beginTx()) {
            Result rs = getDatabase().execute("CALL ga.timetree.events.single(" +
                    "{time: {params}.time, resolution: {params}.resolution, timezone: {params}.timezone, relationshipTypes: [{params}.relationshipType], direction: {params}.direction}) " +
                    "YIELD node, relationshipType, direction RETURN *", getParamsMapForTime(time));
            while (rs.hasNext()) {
                ++i;
                Map<String, Object> record = rs.next();
                Node node  = (Node) record.get("node");
                assertEquals(time, node.getProperty(TIME_PROPERTY));
                String relationshipType = record.get("relationshipType").toString();
                assertEquals(DEFAULT_REL_TYPE, relationshipType);
                String direction = record.get("direction").toString();
                assertEquals(Direction.INCOMING.toString(), direction);
            }
            tx.success();
        }
        assertEquals(1, i);
    }

    @Test
    public void testEventsAutoAttachedCanBeRetrievedViaProcedureWithOnlyTimeInMap() {
        long time = dateToMillis(2015, 1, 3, 16);
        createEvent(time);
        int i = 0;
        try (Transaction tx = getDatabase().beginTx()) {
            Result rs = getDatabase().execute("CALL ga.timetree.events.single(" +
                    "{time: {params}.time}) " +
                    "YIELD node, relationshipType, direction RETURN *", getParamsMapForTime(time));
            while (rs.hasNext()) {
                ++i;
                Map<String, Object> record = rs.next();
                Node node  = (Node) record.get("node");
                assertEquals(time, node.getProperty(TIME_PROPERTY));
                String relationshipType = record.get("relationshipType").toString();
                assertEquals(DEFAULT_REL_TYPE, relationshipType);
                String direction = record.get("direction").toString();
                assertEquals(Direction.INCOMING.toString(), direction);
            }
            tx.success();
        }
        assertEquals(1, i);
    }

    @Test
    public void testMultipleAutoAttachedEventsAreReturnedWithProcedure() {
        long t = dateToMillis(2016, 1, 1, 1);
        for (int i = 0; i < 10; ++i) {
            createEvent(t + i);
        }

        int i = 0;
        try (Transaction tx = getDatabase().beginTx()) {
            Result rs = getDatabase().execute("CALL ga.timetree.events.single(" +
                    "{time: {params}.time, resolution: {params}.resolution, timezone: {params}.timezone, relationshipTypes: [{params}.relationshipType], direction: {params}.direction}) " +
                    "YIELD node, relationshipType, direction RETURN *", getParamsMapForTime(t));
            while (rs.hasNext()) {
                Map<String, Object> record = rs.next();
                ++i;
            }
            tx.success();
        }
        assertEquals(10, i);
    }

    @Test
    public void testMultipleAutoAttachedEventsAreReturnedWithProcedureInRangeAndOnlyFromToInMap() {
        long t = dateToMillis(2016, 1, 1, 1);
        for (int i = 0; i < 10; ++i) {
            createEvent(t + (i*10000));
        }
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> params = new HashMap<>();
        params.put("start", t);
        params.put("end", t + 100000);
        map.put("params", params);

        int i = 0;
        try (Transaction tx = getDatabase().beginTx()) {
            Result rs = getDatabase().execute("CALL ga.timetree.events.range({params}) " +
                    "YIELD node, relationshipType, direction RETURN *", map);
            while (rs.hasNext()) {
                Map<String, Object> record = rs.next();
                ++i;
            }
            tx.success();
        }
        assertEquals(10, i);
    }

    @Test
    public void testRangedEventsReturnedForCustomRoot() {
        long customRootId;
        long time = dateToMillis(2015, 1, 3, 16);
        try (Transaction tx = getDatabase().beginTx()) {
            Node custom = getDatabase().createNode(Label.label("Person"));
            customRootId = custom.getId();
            getDatabase().execute("CREATE (n:Email {time: " + time + ", timeTreeRootId: " + customRootId + "})");
            tx.success();
        }

        int i = 0;
        try (Transaction tx = getDatabase().beginTx()) {
            int p = 0;
            ResourceIterator<Node> nodes = getDatabase().findNodes(Label.label("Person"));
            while (nodes.hasNext()) {
                ++p;
                Node person = nodes.next();
                assertTrue(person.hasRelationship(RelationshipType.withName("CHILD")));
            }
            assertEquals(1, p);

            Result result = getDatabase().execute("MATCH (n:Person) WHERE id(n) = " + customRootId + " " +
                    "CALL ga.timetree.events.range({start: " + time + " - 1000, end: " + time + " + 1000, root: n}) " +
                    "YIELD node RETURN node");
            while (result.hasNext()) {
                Map<String, Object> record = result.next();
                ++i;
                Node e = (Node) record.get("node");
                assertTrue(e.hasRelationship(RelationshipType.withName("SENT_ON")));
            }
            tx.success();
        }
        assertEquals(1, i);
    }

    @Test
    public void testAttachWithCustomRoot() {
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().execute("CREATE (n:Person {name:'me'}) " +
                    "CREATE (e:Event {id: 123}) " +
                    "WITH n, e " +
                    "CALL ga.timetree.events.attach({node: e, root: n, time: timestamp(), relationshipType: 'OCCURED_ON'}) YIELD node RETURN *");
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().findNode(Label.label("Person"), "name", "me");
            Node year = node.getSingleRelationship(RelationshipType.withName("CHILD"), Direction.OUTGOING).getEndNode();
            assertTrue(year.hasLabel(Label.label("Year")));
            Node event = getDatabase().findNode(Label.label("Event"), "id", 123);
            Node instant = event.getSingleRelationship(RelationshipType.withName("OCCURED_ON"), Direction.OUTGOING).getStartNode();
            tx.success();
        }
    }
    
    @Test
    public void testAttach() {
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().execute("CREATE (e:Event {id: 123}) " +
                    "WITH e " +
                    "CALL ga.timetree.events.attach({node: e, time: timestamp(), relationshipType: 'OCCURED_ON'}) YIELD node RETURN *");
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Node event = getDatabase().findNode(Label.label("Event"), "id", 123);
            Node instant = event.getSingleRelationship(RelationshipType.withName("OCCURED_ON"), Direction.OUTGOING).getEndNode();
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
            assertEquals(instant.getProperty("value"), dayOfMonth);
            tx.success();
        }
    }

    @Test
    public void testAttachThrowsExceptionWhenNoRelTypeGiven() {
        try (Transaction tx = getDatabase().beginTx()) {
            try {
                getDatabase().execute("CREATE (ev:Event {name:'event'}) WITH ev CALL ga.timetree.events.attach({node: ev, time: timestamp()}) YIELD node RETURN node");
                assertEquals(1, 2);
            } catch (RuntimeException e) {
                assertTrue(e.getMessage().contains("The given relationship type cannot be null or an empty string"));
            }
            tx.success();
        } catch (TransactionFailureException e) {
            //
        }
    }
    
    @Test
    public void eventAndTimeInstantAtCustomRootShouldBeCreatedWhenEventIsAttached() {
        long customRootId;
        long time = dateToMillis(2015, 1, 3, 16);
        try (Transaction tx = getDatabase().beginTx()) {
            Node custom = getDatabase().createNode(Label.label("Person"));
            customRootId = custom.getId();
            getDatabase().execute("CREATE (n:Email {time: "+ time +", timeTreeRootId: " + customRootId + "})");
            tx.success();
        }        
        int i = 0;
        try (Transaction tx = getDatabase().beginTx()) {
            Result rs = getDatabase().execute("MATCH (n:Person) WHERE id(n) = " + customRootId + " " +
                    "CALL ga.timetree.events.single(" +
                    "{root: n, time: " + time + "}) " +
                    "YIELD node, relationshipType, direction RETURN *");
            while (rs.hasNext()) {
                ++i;
                Map<String, Object> record = rs.next();
                Node node  = (Node) record.get("node");
                assertEquals(time, node.getProperty(TIME_PROPERTY));
                String relationshipType = record.get("relationshipType").toString();
                assertEquals(DEFAULT_REL_TYPE, relationshipType);
                String direction = record.get("direction").toString();
                assertEquals(Direction.INCOMING.toString(), direction);
            }
            tx.success();
        }
        assertEquals(1, i);
    }

    private Map<String, Object> getParamsMapForTime(long time) {
        Map<String, Object> params = new HashMap<>();
        params.put("time", time);
        params.put("resolution", null);
        params.put("timezone", null);
        params.put("relationshipType", null);
        params.put("direction", null);

        Map<String, Object> map = new HashMap<>();
        map.put("params", params);

        return map;
    }
    
    private Map<String, Object> getCompleteParamsMapForTime(long time) {
        Map<String, Object> params = new HashMap<>();
        params.put("time", time);
        params.put("resolution", "DAY");
        params.put("timezone", "UTC");
        params.put("relationshipType", "AT_TIME");
        params.put("direction", null);

        Map<String, Object> map = new HashMap<>();
        map.put("params", params);

        return map;
    }

    private void createEvent(long time) {
        try (Transaction tx = getDatabase().beginTx()) {
            Node n = getDatabase().createNode(Label.label(EMAIL));
            n.setProperty(TIME_PROPERTY, time);
            tx.success();
        }
    }
    
    private String getUrl() {
        return baseUrl() + "/timetree/";
    }

    
    private long dateToMillis(int year, int month, int day, int hour) {
        return dateToDateTime(year, month, day, hour).getMillis();
    }

    private DateTime dateToDateTime(int year, int month, int day, int hour) {
        return new DateTime(year, month, day, hour, 0, DateTimeZone.UTC);
    }

   
}
