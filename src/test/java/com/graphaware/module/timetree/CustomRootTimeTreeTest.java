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

package com.graphaware.module.timetree;

import static com.graphaware.module.timetree.SingleTimeTree.VALUE_PROPERTY;
import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;

import com.graphaware.common.util.EntityUtils;
import com.graphaware.module.timetree.domain.TimeInstant;
import com.graphaware.module.timetree.domain.TimeTreeLabels;
import com.graphaware.test.data.DatabasePopulator;
import com.graphaware.test.data.SingleTransactionPopulator;
import com.graphaware.test.integration.EmbeddedDatabaseIntegrationTest;

/**
 * Unit test for {@link com.graphaware.module.timetree.SingleTimeTree}.
 */
public class CustomRootTimeTreeTest extends EmbeddedDatabaseIntegrationTest {

    private static final DateTimeZone UTC = DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"));

    @Override
    protected DatabasePopulator databasePopulator() {
        return new SingleTransactionPopulator() {
            @Override
            protected void doPopulate(GraphDatabaseService database) {
                database.createNode(Label.label("CustomRoot"));
            }
        };
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequested() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);

        //When
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            TimeTree timeTree = new CustomRootTimeTree(getDatabase().getNodeById(0));
            dayNode = timeTree.getOrCreateInstant(TimeInstant.instant(dateInMillis));
            tx.success();
        }

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
                "(month)-[:LAST]->(day)");

        try (Transaction tx = getDatabase().beginTx()) {
            assertTrue(dayNode.hasLabel(TimeTreeLabels.Day));
            assertEquals(4, dayNode.getProperty(VALUE_PROPERTY));
        }
    }

    @Test(expected = NotFoundException.class)
    public void whenTheRootIsDeletedSubsequentRestApiCallsShouldThrowNotFoundException() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        TimeTree timeTree;
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree = new CustomRootTimeTree(getDatabase().getNodeById(0));
            timeTree.getOrCreateInstant(TimeInstant.instant(dateInMillis));
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            for (Node node : getDatabase().getAllNodes()) {
                EntityUtils.deleteNodeAndRelationships(node);
            }
            tx.success();
        }


        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getOrCreateInstant(TimeInstant.instant(dateInMillis));
            tx.success();
        }
        //NotFoundException should be thrown
    }

    private long dateToMillis(int year, int month, int day) {
        return dateToDateTime(year, month, day).getMillis();
    }

    private DateTime dateToDateTime(int year, int month, int day) {
        return new DateTime(year, month, day, 0, 0, UTC);
    }
}
