/*
 * Copyright (c) 2014 GraphAware
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

package com.graphaware.module.timetree;

import com.graphaware.common.util.PropertyContainerUtils;
import com.graphaware.test.integration.DatabaseIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.TimeZone;

import static com.graphaware.module.timetree.SingleTimeTree.VALUE_PROPERTY;
import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link com.graphaware.module.timetree.SingleTimeTree}.
 */
public class CustomRootTimeTreeTest extends DatabaseIntegrationTest {

    private static final DateTimeZone UTC = DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"));

    @Override
    protected void populateDatabase(GraphDatabaseService database) {
        database.createNode(DynamicLabel.label("CustomRoot"));
    }

    @Test
    public void trivialTreeShouldBeCreatedWhenFirstDayIsRequested() {
        //Given
        long dateInMillis = dateToMillis(2013, 5, 4);

        //When
        Node dayNode;
        try (Transaction tx = getDatabase().beginTx()) {
            TimeTree timeTree = new CustomRootTimeTree(getDatabase().getNodeById(0));
            dayNode = timeTree.getInstant(new TimeInstant(dateInMillis), tx);
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
            timeTree.getInstant(new TimeInstant(dateInMillis), tx);
            tx.success();
        }

        //When
        try (Transaction tx = getDatabase().beginTx()) {
            for (Node node : GlobalGraphOperations.at(getDatabase()).getAllNodes()) {
                PropertyContainerUtils.deleteNodeAndRelationships(node);
            }
            tx.success();
        }


        //Then
        try (Transaction tx = getDatabase().beginTx()) {
            timeTree.getInstant(new TimeInstant(dateInMillis), tx);
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
