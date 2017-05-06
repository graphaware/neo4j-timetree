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

package com.graphaware.module.timetree;

import static com.graphaware.module.timetree.domain.Resolution.YEAR;
import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import com.graphaware.module.timetree.domain.Resolution;
import com.graphaware.module.timetree.domain.TimeInstant;
import com.graphaware.test.integration.EmbeddedDatabaseIntegrationTest;

public class ReadOnlySingleTimeTreeTest extends EmbeddedDatabaseIntegrationTest {

    private TimeTree timeTree; //class under test

    private static final DateTimeZone UTC = DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"));

	private Transaction tx;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        timeTree = new SingleTimeTree(getDatabase());
        tx = getDatabase().beginTx();
    }

    @After
    public void tearDown(){
    	tx.failure();
    	tx.close();
    }
    
    private long dateToMillis(int year, int month, int day) {
        return dateToDateTime(year, month, day).getMillis();
    }

    private DateTime dateToDateTime(int year, int month, int day) {
        return new DateTime(year, month, day, 0, 0, UTC);
    }
    
    @Test
    public void testGetInstantEmpty() {
    	//Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        TimeInstant timeInstant = TimeInstant.instant(dateInMillis).with(YEAR);

        //When
        Node yearNode;
        yearNode = timeTree.getInstant(timeInstant);

        //Then
        assertNull(yearNode);
        assertFalse(getDatabase().getAllNodes().iterator().hasNext());
    }

    @Test
    public void testGetInstantOrAfterEmpty() {
    	//Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        TimeInstant timeInstant = TimeInstant.instant(dateInMillis).with(YEAR);

        //When
        Node yearNode;
        yearNode = timeTree.getInstantAtOrAfter(timeInstant);

        //Then
        assertNull(yearNode);
        assertFalse(getDatabase().getAllNodes().iterator().hasNext());
    }

    @Test
    public void testGetInstantOrBeforeEmpty() {
    	//Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        TimeInstant timeInstant = TimeInstant.instant(dateInMillis).with(YEAR);

        //When
        Node yearNode;
        yearNode = timeTree.getInstantAtOrBefore(timeInstant);

        //Then
        assertNull(yearNode);
        assertFalse(getDatabase().getAllNodes().iterator().hasNext());
    }
    
	private String populateTree(Long year, Long month, Long day) {
		Transaction txc = getDatabase().beginTx();

		String query = "CREATE" +
                "(root:TimeTreeRoot)," +
                "(root)-[:FIRST]->(year:Year {value: "+year+" })," +
                "(root)-[:CHILD]->(year)," +
                "(root)-[:LAST]->(year)," +
                "(year)-[:FIRST]->(month:Month {value: "+month+" })," +
                "(year)-[:CHILD]->(month)," +
                "(year)-[:LAST]->(month)," +
                "(month)-[:FIRST]->(day:Day {value: "+day+" })," +
                "(month)-[:CHILD]->(day)," +
                "(month)-[:LAST]->(day)";
		
		getDatabase().execute(query);
        txc.success();
        txc.close();
        return query;
	}

    private Object[] getInstantParameters(final long year, final long month, final long day) {

        return new Object[]{
                new Object[]{Resolution.YEAR,year},
                new Object[]{Resolution.MONTH,month},
                new Object[]{Resolution.DAY,day},
        };

    }
    
    @Test
    public void testGetInstant() {
    	//Given
        long dateInMillis = dateToMillis(2013, 5, 4);
        
        String tree = populateTree(2013L,5L,4L);
    	
    	//at the time of writing there's not available a JUnitParamsRunner so it uses an homemade trick 
    	
    	Object[] instantParameters = getInstantParameters(2013,5,4);
    	for (Object params : instantParameters) {
    		Object[] val = (Object[]) params;
    		Resolution resolution = (Resolution) val[0];
    		Long result = (Long) val[1];
            
            //When
    		TimeInstant timeInstant = TimeInstant.instant(dateInMillis).with(resolution);
            Node node=  timeTree.getInstant(timeInstant);

            //Then
            assertNotNull("Node not found at resolution: "+resolution,node);
            assertEquals("Wrong resolution: "+resolution,result, node.getProperty("value"));
		}
    	
    	//nothing is changed 
    	assertSameGraph(getDatabase(), tree);
    }
    
    @Test
    public void testGetInstantBefore() {
    	
    	//Given
        long dateInMillis = dateToMillis(2013, 5, 5);
        
        String tree = populateTree(2013L,5L,4L);
    	
    	//at the time of writing there's not available a JUnitParamsRunner so it uses an homemade trick 
    	
    	Object[] instantParameters = getInstantParameters(2013,5,4);
    	for (Object params : instantParameters) {
    		Object[] val = (Object[]) params;
    		Resolution resolution = (Resolution) val[0];
    		Long result = (Long) val[1];
            
            //When
    		TimeInstant timeInstant = TimeInstant.instant(dateInMillis).with(resolution);
            Node node=  timeTree.getInstantAtOrBefore(timeInstant);

            //Then
            assertNotNull("Node not found at resolution: "+resolution,node);
            assertEquals("Wrong resolution: "+resolution,result, node.getProperty("value"));
		}

    	//nothing is changed 
    	assertSameGraph(getDatabase(), tree);
    }

    @Test
    public void testGetInstantAfter() {
    	
    	//Given
        long dateInMillis = dateToMillis(2013, 5, 3);
        
        String tree = populateTree(2013L,5L,4L);
    	
    	//at the time of writing there's not available a JUnitParamsRunner so it uses an homemade trick 
    	
    	Object[] instantParameters = getInstantParameters(2013,5,4);
    	for (Object params : instantParameters) {
    		Object[] val = (Object[]) params;
    		Resolution resolution = (Resolution) val[0];
    		Long result = (Long) val[1];
            
            //When
    		TimeInstant timeInstant = TimeInstant.instant(dateInMillis).with(resolution);
            Node node=  timeTree.getInstantAtOrAfter(timeInstant);

            //Then
            assertNotNull("Node not found at resolution: "+resolution,node);
            assertEquals("Wrong resolution: "+resolution,result, node.getProperty("value"));
		}

    	//nothing is changed 
    	assertSameGraph(getDatabase(), tree);
    }


    @Test
    public void testGetInstantsRange() {
    	
    	//Given
        long dateFrom = dateToMillis(2013, 5, 3);
        long dateTo = dateToMillis(2013, 5, 5);
        
        String tree = populateTree(2013L,5L,4L);
    	
    	//at the time of writing there's not available a JUnitParamsRunner so it uses an homemade trick 
    	
    	Object[] instantParameters = getInstantParameters(2013,5,4);
    	for (Object params : instantParameters) {
    		Object[] val = (Object[]) params;
    		Resolution resolution = (Resolution) val[0];
    		Long result = (Long) val[1];
            
            //When
    		TimeInstant tf = TimeInstant.instant(dateFrom).with(resolution);
    		TimeInstant tt = TimeInstant.instant(dateTo).with(resolution);
    		List<Node> instants = timeTree.getInstants(tf, tt);

    		assertNotNull(instants);
    		assertEquals(1, instants.size());
    		Node node = instants.get(0);
            //Then
            assertNotNull("Node not found at resolution: "+resolution,node);
            assertEquals("Wrong resolution: "+resolution,result, node.getProperty("value"));
		}
    	
    	//nothing is changed 
    	assertSameGraph(getDatabase(), tree);
    }
}
