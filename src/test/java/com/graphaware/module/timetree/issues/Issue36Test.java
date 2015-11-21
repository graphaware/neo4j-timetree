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

package com.graphaware.module.timetree.issues;

import com.graphaware.test.integration.CommunityNeoServerIntegrationTest;
import org.apache.http.HttpStatus;
import org.junit.Test;

public class Issue36Test extends CommunityNeoServerIntegrationTest {

    @Override
    protected String neo4jConfigFile() {
        return "issue-36.properties";
    }

    @Test
    public void verifyIssue36IsNotAnIssue() throws InterruptedException {
        httpClient.executeCypher(baseUrl(), "CREATE (n:SomeNode)"); //will get ID 0
        httpClient.executeCypher(baseUrl(), "CREATE (n:Customer {timestamp: 1436941284 , timeTreeRootId: 0, name: 'test'}) return n");

        String cypher = "CREATE (n:SomeNode), (y:Year {value:1970}), (m:Month {value:1}), (d:Day {value:17}), (h:Hour {value:15})," +
                "(n)-[:LAST]->(y)," +
                "(n)-[:FIRST]->(y)," +
                "(n)-[:CHILD]->(y)," +
                "(y)-[:LAST]->(m)," +
                "(y)-[:FIRST]->(m)," +
                "(y)-[:CHILD]->(m)," +
                "(m)-[:LAST]->(d)," +
                "(m)-[:FIRST]->(d)," +
                "(m)-[:CHILD]->(d)," +
                "(d)-[:LAST]->(h)," +
                "(d)-[:FIRST]->(h)," +
                "(d)-[:CHILD]->(h)," +
                "(h)<-[:CREATED_ON]-(:Customer {timestamp: 1436941284 , timeTreeRootId: 0, name: 'test'})";

        httpClient.post(baseUrl() + "/graphaware/resttest/assertSameGraph", "{\"cypher\":\"" + cypher + "\"}", HttpStatus.SC_OK);
    }
}
