/*
 * Copyright (c) 2013-2017 GraphAware
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
import com.graphaware.test.unit.GraphUnit;
import org.apache.http.HttpStatus;
import org.json.JSONException;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Issue72Test extends GraphAwareIntegrationTest {

    @Override
    protected String configFile() {
        return "issue-72.properties";
    }

    @Test
    public void verifyIssue72() throws InterruptedException {
        httpClient.executeCypher(baseNeoUrl(), "CREATE(:Machine {serial:'123456'})<-[:owns]-(:HostNode {name:'Sample'})");
        httpClient.executeCypher(baseNeoUrl(), "MATCH(m:Machine {serial:'123456'})<-[:owns]-(:HostNode {name:'Sample'}) create (f:Fault {dateOpened:1511883400000, dateClosed:1511883600000, description:'Dummy Fault Testing Feature'})-[:on]->(m) return f");

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().findNodes(Label.label("Fault")).stream().forEach(node -> {
                assertTrue(node.hasRelationship(RelationshipType.withName("faultStarted")));
                assertTrue(node.hasRelationship(RelationshipType.withName("faultEnded")));
            });
        }
    }
}
