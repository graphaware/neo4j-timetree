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

package com.graphaware.module.timetree.proc;

import com.graphaware.module.timetree.domain.Resolution;
import com.graphaware.module.timetree.domain.TimeInstant;
import com.graphaware.test.integration.GraphAwareIntegrationTest;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.neo4j.graphdb.*;

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
            Result result = getDatabase().execute("CALL ga.timetree.events.single({time}, null, null, null, null) YIELD node, relationshipType, direction", params);
            int count = 0;
            while (result.hasNext()) {
                Map<String, Object> next = result.next();
                assertNotNull(next.get("node"));
                assertTrue(
                        RelationshipType.withName("AT_TIME").equals(next.get("relationshipType")) ||
                        RelationshipType.withName("AT_BAD_TIME").equals(next.get("relationshipType")) ||
                        RelationshipType.withName("AT_OTHER_TIME").equals(next.get("relationshipType"))
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
        long time = System.currentTimeMillis();
        createEvent(time);
        int i = 0;
        try (Transaction tx = getDatabase().beginTx()) {
            Result rs = getDatabase().execute("CALL ga.timetree.events.single(" +
                    "{params}.time, {params}.resolution, {params}.timezone, {params}.relationshipType, {params}.direction) " +
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
    
    private long dateToMillis(int year, int month, int day) {
        return dateToDateTime(year, month, day).getMillis();
    }

    private DateTime dateToDateTime(int year, int month, int day) {
        return new DateTime(year, month, day, 0, 0, DateTimeZone.UTC);
    }

   
}
