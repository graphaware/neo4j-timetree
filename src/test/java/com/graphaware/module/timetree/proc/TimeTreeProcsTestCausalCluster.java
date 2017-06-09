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

import static org.junit.Assert.*;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import com.graphaware.module.timetree.module.TimeTreeConfiguration;
import com.graphaware.module.timetree.module.TimeTreeModule;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.test.integration.cluster.CausalClusterDatabasesintegrationTest;

/**
 * Test the read-only procedures in all kind of instances - Causal Cluster
 * ga.timetree.now - in no-leader instance threw write-permission exception
 */
public class TimeTreeProcsTestCausalCluster extends CausalClusterDatabasesintegrationTest {

	@Override
	protected boolean shouldRegisterModules() {
		return true;
	}

	@Override
	protected void registerModules(GraphDatabaseService database) throws Exception {
		GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
		runtime.registerModule(new TimeTreeModule("timetree", TimeTreeConfiguration.defaultConfiguration(), database));
		runtime.start();

		TimeTreeProcedures.register(database);
	}

	@Test
	public void testNowReadOnly_LEADER() {
		GraphDatabaseService db = getLeaderDatabase();
		Result execute = db.execute("CALL ga.timetree.now({create: false}) YIELD instant RETURN instant");
		assertFalse(execute.hasNext());
	}

	@Test
	public void testNowReadOnly_FOLLOWER() {
		GraphDatabaseService db = getOneFollowerDatabase();
		Result execute = db.execute("CALL ga.timetree.now({create: false}) YIELD instant RETURN instant");
		assertFalse(execute.hasNext());
	}
	
	@Test
	public void testNowReadOnly_REPLICA() {
		GraphDatabaseService db = getOneReplicaDatabase();
		Result execute = db.execute("CALL ga.timetree.now({create: false}) YIELD instant RETURN instant");
		assertFalse(execute.hasNext());
	}

	@Test
	public void testGetReadOnly_LEADER() {
		GraphDatabaseService db = getLeaderDatabase();
		Result execute = db.execute("CALL ga.timetree.single({time: 1463659567468}) YIELD instant RETURN instant");
		assertFalse(execute.hasNext());
	}

	@Test
	public void testGetReadOnly_FOLLOWER() {
		GraphDatabaseService db = getOneFollowerDatabase();
		Result execute = db.execute("CALL ga.timetree.single({time: 1463659567468}) YIELD instant RETURN instant");
		assertFalse(execute.hasNext());
	}

	@Test
	public void testGetReadOnly_REPLICA() {
		GraphDatabaseService db = getOneReplicaDatabase();
		Result execute = db.execute("CALL ga.timetree.single({time: 1463659567468}) YIELD instant RETURN instant");
		assertFalse(execute.hasNext());
	}

	@Test
	public void testRangeReadOnly_LEADER() {
		GraphDatabaseService db = getLeaderDatabase();
		Result execute = db
				.execute("CALL ga.timetree.range({start: 1463659567468, end: 1463859569504, create: false})");
		assertFalse(execute.hasNext());
	}

	@Test
	public void testRangeReadOnly_FOLLOWER() {
		GraphDatabaseService db = getOneFollowerDatabase();
		Result execute = db
				.execute("CALL ga.timetree.range({start: 1463659567468, end: 1463859569504, create: false})");
		assertFalse(execute.hasNext());
	}

	@Test
	public void testRangeReadOnly_REPLICA() {
		GraphDatabaseService db = getOneReplicaDatabase();
		Result execute = db
				.execute("CALL ga.timetree.range({start: 1463659567468, end: 1463859569504, create: false})");
		assertFalse(execute.hasNext());
	}
}
