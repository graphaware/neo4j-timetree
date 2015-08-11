/*
 * Copyright (c) 2015 GraphAware
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

import com.graphaware.common.policy.InclusionPolicies;
import com.graphaware.common.policy.NodePropertyInclusionPolicy;
import com.graphaware.common.util.PropertyContainerUtils;
import com.graphaware.test.integration.GraphAwareApiTest;
import org.apache.http.HttpStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.concurrent.atomic.AtomicInteger;

import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static com.graphaware.test.unit.GraphUnit.printGraph;
import static org.junit.Assert.assertEquals;

/**
 * Integration test for {@link TimeTreeApi}.
 */
public class TimeTreeApiWithUUIDTest extends GraphAwareApiTest {

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
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequested() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);

        //When
        String result = httpClient.get(getUrl() + "single/" + dateInMillis, HttpStatus.SC_OK);

        //Then
        assertSameGraph(getDatabase(), "CREATE" +
                "(root:TimeTreeRoot {uuid:'test-uuid-1'})," +
                "(root)-[:FIRST]->(year:Year {value:2013, uuid: 'test-uuid-2'})," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value:5, uuid: 'test-uuid-3'})," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value:4, uuid: 'test-uuid-4'})," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)");

        assertEquals("{\"id\":3,\"properties\":{\"value\":4,\"uuid\":\"test-uuid-4\"},\"labels\":[\"Day\"]}", result);
    }

    @Test
    public void consecutiveDaysShouldBeCreatedWhenRequested() {

        //Given
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);

        //When
        String result = httpClient.get(getUrl() + "range/" + startDateInMillis + "/" + endDateInMillis, HttpStatus.SC_OK);

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
                "(day6)-[:NEXT]->(day7)", ignoreUuid);

        assertEquals("[{\"id\":3,\"properties\":{\"value\":4,\"uuid\":\"test-uuid-4\"},\"labels\":[\"Day\"]},{\"id\":4,\"properties\":{\"value\":5,\"uuid\":\"test-uuid-5\"},\"labels\":[\"Day\"]},{\"id\":5,\"properties\":{\"value\":6,\"uuid\":\"test-uuid-6\"},\"labels\":[\"Day\"]},{\"id\":6,\"properties\":{\"value\":7,\"uuid\":\"test-uuid-7\"},\"labels\":[\"Day\"]}]", result);
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

        String result = httpClient.get(getUrl() + "0/single/" + dateInMillis, HttpStatus.SC_OK);

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
                "(month)-[:LAST]->(day)", ignoreUuid);

        assertEquals("{\"id\":3,\"properties\":{\"value\":4,\"uuid\":\"test-uuid-4\"},\"labels\":[\"Day\"]}", result);
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

        String result = httpClient.get(getUrl() + "0/range/" + startDateInMillis + "/" + endDateInMillis, HttpStatus.SC_OK);

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
                "(day6)-[:NEXT]->(day7)", ignoreUuid);

        assertEquals("[{\"id\":3,\"properties\":{\"value\":4,\"uuid\":\"test-uuid-4\"},\"labels\":[\"Day\"]},{\"id\":4,\"properties\":{\"value\":5,\"uuid\":\"test-uuid-5\"},\"labels\":[\"Day\"]},{\"id\":5,\"properties\":{\"value\":6,\"uuid\":\"test-uuid-6\"},\"labels\":[\"Day\"]},{\"id\":6,\"properties\":{\"value\":7,\"uuid\":\"test-uuid-7\"},\"labels\":[\"Day\"]}]", result);
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenTodayIsRequested() {
        //Given
        DateTime now = DateTime.now(DateTimeZone.UTC);

        //When
        String result = httpClient.get(getUrl() + "now", HttpStatus.SC_OK);

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
                "(month)-[:LAST]->(day)", ignoreUuid);

        assertEquals("{\"id\":3,\"properties\":{\"value\":" + now.getDayOfMonth() + ",\"uuid\":\"test-uuid-4\"},\"labels\":[\"Day\"]}", result);
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

        String result = httpClient.get(getUrl() + "/0/now", HttpStatus.SC_OK);

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
                "(month)-[:LAST]->(day)", ignoreUuid);

        assertEquals("{\"id\":3,\"properties\":{\"value\":" + now.getDayOfMonth() + ",\"uuid\":\"test-uuid-4\"},\"labels\":[\"Day\"]}", result);
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
