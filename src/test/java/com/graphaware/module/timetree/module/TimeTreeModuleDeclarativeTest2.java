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

package com.graphaware.module.timetree.module;

import com.graphaware.test.integration.GraphAwareIntegrationTest;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;

import static com.graphaware.test.unit.GraphUnit.assertSameGraph;

/**
 * Integration test for {@link TimeTreeModule}.
 */
public class TimeTreeModuleDeclarativeTest2 extends GraphAwareIntegrationTest {

    private static final long TIMESTAMP;

    static {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        c.set(2015, Calendar.APRIL, 5, 12, 55, 22);
        TIMESTAMP = c.getTimeInMillis();
    }

    @Override
    protected String configFile() {
        return "neo4j-timetree2.properties";
    }

    @Test
    public void eventAndTimeInstantShouldBeCreatedWhenEventIsAttached() throws IOException {
        httpClient.executeCypher(baseNeoUrl(), "CREATE (:Email {subject:'Neo4j', time:" + TIMESTAMP + "})");

        assertSameGraph(getDatabase(), "CREATE " +
                        "(event:Email {subject:'Neo4j', time:" + TIMESTAMP + "})," +
                        "(root:TimeTreeRoot)," +
                        "(root)-[:FIRST]->(year:Year {value:2015})," +
                        "(root)-[:CHILD]->(year)," +
                        "(root)-[:LAST]->(year)," +
                        "(year)-[:FIRST]->(month:Month {value:4})," +
                        "(year)-[:CHILD]->(month)," +
                        "(year)-[:LAST]->(month)," +
                        "(month)-[:FIRST]->(day:Day {value:5})," +
                        "(month)-[:CHILD]->(day)," +
                        "(month)-[:LAST]->(day)," +
                        "(day)-[:FIRST]->(hour:Hour {value:13})," + //1 hour more!
                        "(day)-[:CHILD]->(hour)," +
                        "(day)-[:LAST]->(hour)," +
                        "(hour)-[:SENT_ON]->(event)"
        );
    }
}
