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

package com.graphaware.module.timetree;

import com.graphaware.test.integration.NeoServerIntegrationTest;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * {@link NeoServerIntegrationTest} for {@link TimeTree} module and {@link com.graphaware.module.timetree.api.TimeTreeApi}.
 */
public class TimeTreeIntegrationTest extends NeoServerIntegrationTest {

    @Test
    public void graphAwareApisAreMountedWhenPresentOnClasspath() throws InterruptedException, IOException {
        httpClient.post(baseUrl() + "/graphaware/timetree/now/", HttpStatus.OK_200);
    }

    @Test
    public void verifyLotsOfConcurrentRequestsDoNotCauseExceptions() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(20);
        int noRequests = 1000;
        final AtomicInteger successfulRequests = new AtomicInteger(0);

        for (int i = 0; i < noRequests; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    httpClient.post(baseUrl() + "/graphaware/timetree/now?resolution=millisecond", HttpStatus.OK_200);
                    successfulRequests.incrementAndGet();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        assertEquals(noRequests, successfulRequests.get());

        //make sure there's only 1 root
        assertEquals("{\"results\":[{\"columns\":[\"count(n)\"],\"data\":[{\"row\":[1]}]}],\"errors\":[]}", httpClient.executeCypher(baseUrl(), "MATCH (n:TimeTreeRoot) RETURN count(n)"));
    }

    @Test
    public void shouldReturnEvents() {
        String nodeId = httpClient.post(baseUrl() + "/graphaware/timetree/now?resolution=second", HttpStatus.OK_200);

        httpClient.executeCypher(baseUrl(), "START second=node(" + nodeId + ") " +
                "CREATE (email:Event {subject:'Neo4j'})-[:SENT_ON]->(second)");

        long now = new Date().getTime();

        String actual = httpClient.get(baseUrl() + "/graphaware/timetree/range/" + (now - 100000000000L) + "/" + (now + 100000000000L) + "/events?resolution=second", HttpStatus.OK_200);
        assertNotSame("[]", actual);
    }
}
