package com.graphaware.module.timetree;

import com.graphaware.test.integration.NeoServerIntegrationTest;
import org.apache.http.HttpStatus;
import org.junit.Test;

/**
 * Created by cw on 30/07/15.
 */
public class Issue38Test extends NeoServerIntegrationTest{

    @Override
    protected String neo4jConfigFile() {
        return "issue-38.properties";
    }

    @Test
    public void verifyIssue38() throws InterruptedException {
        httpClient.executeCypher(baseUrl(), "CREATE (n:SomeNode)"); //will get ID 0
        httpClient.executeCypher(baseUrl(), "CREATE (n:Item {uid: '123-fff-456-ggg', created: 1438248945000 , name: 'test'}) return n");
        httpClient.executeCypher(baseUrl(), "match (item:Item {uid:'123-fff-456-ggg'}) SET item.modified = 1438259108000");

        String cypher = "CREATE (n:SomeNode), (y:Year {value:2015}), (m:Month {value:7}), (d:Day {value:30}), (h:Hour {value:20}), (h2:Hour {value:23})," +
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
                "(d)-[:NEXT]->(h2)," +
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

        httpClient.post(baseUrl() + "/graphaware/resttest/assertSameGraph", "{\"cypher\":\"" + cypher + "\"}", HttpStatus.SC_OK);

    }
}
