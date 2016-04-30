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

package com.graphaware.module.timetree.issues;

import com.graphaware.test.integration.GraphAwareIntegrationTest;
import org.apache.http.HttpStatus;
import org.json.JSONException;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import static org.junit.Assert.assertEquals;

public class Issue38Test extends GraphAwareIntegrationTest {

    @Override
    protected String configFile() {
        return "issue-38.properties";
    }

    @Test
    public void verifyIssue38() throws InterruptedException {
        httpClient.executeCypher(baseNeoUrl(), "CREATE (n:Item {uid: '123-fff-456-ggg', created: 1438248945000 , name: 'test'}) return n");
        httpClient.executeCypher(baseNeoUrl(), "match (item:Item {uid:'123-fff-456-ggg'}) SET item.modified = 1438259108000");

        String cypher = "CREATE (n:TimeTreeRoot), (y:Year {value:2015}), (m:Month {value:7}), (d:Day {value:30}), (h:Hour {value:20}), (h2:Hour {value:23})," +
                "(minute:Minute {value:35}), (minute2:Minute {value:25})," +
                "(second:Second {value:45}), (second2:Second {value:8})," +
                "(n)-[:LAST]->(y)," +
                "(n)-[:FIRST]->(y)," +
                "(n)-[:CHILD]->(y)," +
                "(y)-[:LAST]->(m)," +
                "(y)-[:FIRST]->(m)," +
                "(y)-[:CHILD]->(m)," +
                "(m)-[:LAST]->(d)," +
                "(m)-[:FIRST]->(d)," +
                "(m)-[:CHILD]->(d)," +
                "(d)-[:LAST]->(h2)," +
                "(d)-[:FIRST]->(h)," +
                "(h)-[:NEXT]->(h2)," +
                "(d)-[:CHILD]->(h)," +
                "(d)-[:CHILD]->(h2)," +
                "(h)-[:LAST]->(minute)," +
                "(h)-[:FIRST]->(minute)," +
                "(h)-[:CHILD]->(minute)," +
                "(h2)-[:LAST]->(minute2)," +
                "(h2)-[:FIRST]->(minute2)," +
                "(h2)-[:CHILD]->(minute2)," +
                "(minute)-[:NEXT]->(minute2)," +
                "(minute)-[:LAST]->(second)," +
                "(minute)-[:FIRST]->(second)," +
                "(minute)-[:CHILD]->(second)," +
                "(minute2)-[:LAST]->(second2)," +
                "(minute2)-[:FIRST]->(second2)," +
                "(minute2)-[:CHILD]->(second2)," +
                "(second)-[:NEXT]->(second2)," +
                "(second)<-[:Created]-(item:Item {uid: '123-fff-456-ggg', created: 1438248945000 , modified: 1438259108000, name: 'test'})," +
                "(item)-[:Modified]->(second2)";

        httpClient.post(baseUrl() + "/resttest/assertSameGraph", "{\"cypher\":\"" + cypher + "\"}", HttpStatus.SC_OK);
    }

    @Test
    public void testNonUTCEventsAreReturned() throws JSONException {
        // 24.08.2015 11:43UTC
        // hour node will be 22 because configuration is GMT+11
        // node Id will be 0
        httpClient.executeCypher(baseNeoUrl(), "CREATE (n:Item {created: 1440416586000})");
        String response = httpClient.get(baseUrl() + "/timetree/range/1440416586000/1440416600000/events?timezone=GMT+11", null, HttpStatus.SC_OK);
        JSONAssert.assertEquals("[{\"node\":{\"id\":0,\"properties\":{\"created\":1440416586000},\"labels\":[\"Item\"]},\"relationshipType\":\"Created\",\"direction\":\"INCOMING\"}]", response, false);
        // add a node modification property
        // modification time is 24.08.2015 12:02UTC
        httpClient.executeCypher(baseNeoUrl(), "MATCH (n) WHERE id(n) = 0 SET n.modified = 1440417779000");
        String modificationResponse = httpClient.get(baseUrl() + "/timetree/range/1440417779000/1440417779000/events?timezone=GMT+11", null, HttpStatus.SC_OK);
        JSONAssert.assertEquals("[{\"node\":{\"id\":0,\"properties\":{\"created\":1440416586000,\"modified\":1440417779000},\"labels\":[\"Item\"]},\"relationshipType\":\"Created\",\"direction\":\"INCOMING\"},{\"node\":{\"id\":0,\"properties\":{\"created\":1440416586000,\"modified\":1440417779000},\"labels\":[\"Item\"]},\"relationshipType\":\"Modified\",\"direction\":\"INCOMING\"}]", modificationResponse, false);

        // The below query shouldn't return anything as the default timezone is UTC and the previous events were inserted with GMT+11
        String responseFromRequestWithoutTZ = httpClient.get(baseUrl() + "/timetree/range/1440417779000/1440417779000/events?resolution=Second", null, HttpStatus.SC_OK);
        assertEquals("[]", responseFromRequestWithoutTZ);
    }

    @Test
    public void brettoTest() {
        httpClient.executeCypher(baseNeoUrl(), "CREATE (n:Item {created: 1439881228448})");
        httpClient.executeCypher(baseNeoUrl(), "MATCH (n:Item) SET n.modified=1439882928756");
        System.out.println(httpClient.executeCypher(baseNeoUrl(), "MATCH n RETURN n, labels(n)"));
        String response = httpClient.get(baseUrl() + "/timetree/range/1439883028756/1439884091330/events?resolution=Second", null, HttpStatus.SC_OK);
        assertEquals("[]", response);
    }
}
