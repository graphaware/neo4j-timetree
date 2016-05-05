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
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

/**
 * Procedure test for {@link com.graphaware.module.timetree.proc.TimeTreeProcedure}.
 */
public class TimedEventsProcedureTest extends GraphAwareIntegrationTest {

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
