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

import static com.graphaware.module.timetree.domain.TimeTreeLabels.TimeTreeRoot;
import static com.graphaware.module.timetree.domain.TimeTreeRelationshipTypes.CHILD;
import static org.neo4j.graphdb.Direction.INCOMING;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;

import com.graphaware.common.log.LoggerFactory;
import com.graphaware.common.util.IterableUtils;
import com.graphaware.module.timetree.domain.TimeInstant;

/**
 * Facade for {@link TimeTree}, delegating to {@link DefaultSingleTimeTree} implementation.
 */
public class SingleTimeTree implements TimeTree {

	/*	2017-03 - Causal Cluster
	 * 	Introducing causal cluster compatibility, this implementation changed in order to have readonly operations.
	 * 	Readonly operations can run in FOLLOWER and (most important) in READ_REPLICA instances: this means to scale the readers.
	 */
	
    private static final Log LOG = LoggerFactory.getLogger(SingleTimeTree.class);

    protected static final String VALUE_PROPERTY = DefaultSingleTimeTree.VALUE_PROPERTY;
    
    /**
     * Used when create=false
     */
    private TimeTree readOnlyTree;
    
    private TimeTree writableTree;
    
    enum ChildNotFoundPolicy {
        RETURN_NULL, RETURN_PREVIOUS, RETURN_NEXT
    }

    /**
     * Time tree without root management
     * @param database to talk to.
     * @param rootLocator custom root-node retrieving algo
     */
    public SingleTimeTree(GraphDatabaseService database, TimeRootLocator rootLocator) {
    	this.readOnlyTree = new DefaultSingleTimeTree(database, rootLocator);
    	this.writableTree = this.readOnlyTree;
    }
    
    /**
     * Constructor for time tree.
     *
     * @param database to talk to.
     */
    public SingleTimeTree(GraphDatabaseService database) {
    	readOnlyTree = new DefaultSingleTimeTree(database, new TimeRootLocator() {
			
			@Override
			public Node getTimeRoot(GraphDatabaseService database, ReentrantLock rootLock) {
		        Node timeTreeRoot = IterableUtils.getSingleOrNull(database.findNodes(TimeTreeRoot));

		        if (timeTreeRoot != null) {
		            try {
		                timeTreeRoot.getDegree();
		                return timeTreeRoot;
		            } catch (NotFoundException e) {
		                //ok
		            }
		        }

		        return timeTreeRoot;
			}
		});
    	
    	writableTree = new DefaultSingleTimeTree(database, new TimeRootLocator() {
			
			@Override
			public Node getTimeRoot(GraphDatabaseService database, ReentrantLock rootLock) {
		        Node timeTreeRoot = IterableUtils.getSingleOrNull(database.findNodes(TimeTreeRoot));

		        if (timeTreeRoot != null) {
		            try {
		                timeTreeRoot.getDegree();
		                return timeTreeRoot;
		            } catch (NotFoundException e) {
		                //ok
		            }
		        }

		        rootLock.lock();

		        timeTreeRoot = IterableUtils.getSingleOrNull(database.findNodes(TimeTreeRoot));

		        if (timeTreeRoot != null) {
		            rootLock.unlock();
		            return timeTreeRoot;
		        }

		        LOG.info("Creating time tree root");
		        timeTreeRoot = database.createNode(TimeTreeRoot);

		        return timeTreeRoot;
			}
		});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node getInstant(TimeInstant timeInstant) {
    	return this.readOnlyTree.getInstant(timeInstant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node getInstantAtOrAfter(TimeInstant timeInstant) {
    	return this.readOnlyTree.getInstantAtOrAfter(timeInstant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node getInstantAtOrBefore(TimeInstant timeInstant) {
    	return this.readOnlyTree.getInstantAtOrBefore(timeInstant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Node> getInstants(TimeInstant startTime, TimeInstant endTime) {
        return this.readOnlyTree.getInstants(startTime, endTime);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node getOrCreateInstant(TimeInstant timeInstant) {
        return this.writableTree.getOrCreateInstant(timeInstant);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Node> getOrCreateInstants(TimeInstant startTime, TimeInstant endTime) {
        return this.writableTree.getOrCreateInstants(startTime, endTime);
    }

 
    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAll() {
    	this.writableTree.removeAll();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void removeInstant(Node instantNode) {
    	this.writableTree.removeInstant(instantNode);
    }

    /**
     * Find the parent of a node.
     *
     * @param node to find a parent for.
     * @return parent.
     * @throws IllegalStateException in case the node has no parent.
     */
    static Node parent(Node node) {
        Relationship parentRelationship = node.getSingleRelationship(CHILD, INCOMING);

        if (parentRelationship == null) {
            LOG.error(node + " has no parent!");
            throw new IllegalStateException(node + " has no parent!");
        }

        return parentRelationship.getStartNode();
    }

}
