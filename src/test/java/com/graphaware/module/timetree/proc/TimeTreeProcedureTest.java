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

import com.graphaware.test.integration.GraphAwareIntegrationTest;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

/**
 * Procedure test for {@link com.graphaware.module.timetree.proc.TimeTreeProcedure}.
 */
public class TimeTreeProcedureTest extends GraphAwareIntegrationTest {

    @Test
    public void testGetOrCreateInstant() throws JSONException {
        long dateInMillis = dateToMillis(2013, 5, 5);
        Map<String, Object> params = new HashMap<>();
        params.put("time", dateInMillis);
        params.put("resolution", null);
        params.put("timezone", null);
        try( Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.single({time: {time}, resolutiopn: {resolution}, timezone: {timezone}, create: true}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            resIterator.stream().forEach((node) -> assertEquals(node.getProperty("value"), 5));
            tx.success();
        }
        
    }
    
    @Test
    public void testGetInstant() throws JSONException {
        long dateInMillis = dateToMillis(2013, 5, 4);
        Map<String, Object> params = new HashMap<>();
        params.put("time", dateInMillis);
        params.put("resolution", null);
        params.put("timezone", null);
        try( Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.single({time: {time}, resolutiopn: {resolution}, timezone: {timezone}}) YIELD instant return instant", params);
            ResourceIterator<Node> resIterator = result.columnAs("instant");
            assertFalse(resIterator.hasNext());
            tx.success();
        }
        
    }
    
    @Test
    public void testGetInstants() throws JSONException {
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);
        Map<String, Object> params = new HashMap<>();
        params.put("startTime", startDateInMillis);
        params.put("endTime", endDateInMillis);
        params.put("resolution", null);
        params.put("timezone", null);
        try( Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.range({start: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}}) YIELD instants return instants", params);
            ResourceIterator<Node> resIterator = result.columnAs("instants");
            assertFalse(resIterator.hasNext());
            tx.success();
        }
        
    }
    
    @Test
    public void testGetOrCreateInstants() throws JSONException {
        long startDateInMillis = dateToMillis(2013, 5, 4);
        long endDateInMillis = dateToMillis(2013, 5, 7);
        Map<String, Object> params = new HashMap<>();
        params.put("startTime", startDateInMillis);
        params.put("endTime", endDateInMillis);
        params.put("resolution", null);
        params.put("timezone", null);
        try( Transaction tx = getDatabase().beginTx()) {
            Result result = getDatabase().execute("CALL ga.timetree.range({start: {startTime}, end: {endTime}, resolution: {resolution}, timezone: {timezone}, create: true}) YIELD instants return instants", params);
            ResourceIterator<Node> resIterator = result.columnAs("instants");
            long count = resIterator.stream().count();
            assertEquals(4, count);
            tx.success();
        }
        
    }
    private long dateToMillis(int year, int month, int day) {
        return dateToDateTime(year, month, day).getMillis();
    }

    private DateTime dateToDateTime(int year, int month, int day) {
        return new DateTime(year, month, day, 0, 0, DateTimeZone.UTC);
    }

   
}
