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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.graphaware.module.timetree.Resolution.*;
import static com.graphaware.module.timetree.TimeTreeLabels.TimeTreeRoot;
import static com.graphaware.module.timetree.TimeTreeRelationshipTypes.*;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Default implementation of {@link TimeTree}, which builds a single tree and maintains its own root.
 * The default {@link Resolution}, if one is not explicitly provided using the constructor or one of the public methods,
 * is {@link Resolution#DAY}. The default {@link DateTimeZone}, if one is not explicitly provided, is UTC.
 */
public class SingleTimeTree implements TimeTree {
    private static final Logger LOG = LoggerFactory.getLogger(SingleTimeTree.class);

    private static final Resolution DEFAULT_RESOLUTION = DAY;
    private static final DateTimeZone DEFAULT_TIME_ZONE = DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"));
    protected static final String VALUE_PROPERTY = "value";

    private final GraphDatabaseService database;
    private final DateTimeZone timeZone;
    private final Resolution resolution;

    private volatile Node timeTreeRoot;

    /**
     * Constructor for time tree with default {@link Resolution#DAY} resolution and default UTC timezone.
     *
     * @param database to talk to.
     */
    public SingleTimeTree(GraphDatabaseService database) {
        this(database, DEFAULT_RESOLUTION);
    }

    /**
     * Constructor for time tree with default UTC timezone.
     *
     * @param database   to talk to.
     * @param resolution default resolution.
     */
    public SingleTimeTree(GraphDatabaseService database, Resolution resolution) {
        this(database, DEFAULT_TIME_ZONE, resolution);
    }

    /**
     * Constructor for time tree with default {@link Resolution#DAY} resolution.
     *
     * @param database to talk to.
     * @param timeZone default time zone.
     */
    public SingleTimeTree(GraphDatabaseService database, DateTimeZone timeZone) {
        this(database, timeZone, DAY);
    }

    /**
     * Constructor for time tree.
     *
     * @param database   to talk to.
     * @param timeZone   default time zone.
     * @param resolution default resolution.
     */
    public SingleTimeTree(GraphDatabaseService database, DateTimeZone timeZone, Resolution resolution) {
        this.database = database;
        this.timeZone = timeZone;
        this.resolution = resolution;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node getNow(Transaction tx) {
        TimeInstant timeInstant = new TimeInstant();
        return getNow(timeInstant, tx);
    }


    @Override
    public Node getNow(TimeInstant timeInstant, Transaction tx) {
        return getInstant(timeInstant, tx);
    }


    private Node getInstant(long time, DateTimeZone timeZone, Resolution resolution, Transaction tx) {
        if (timeZone == null) {
            timeZone = this.timeZone;
        }

        if (resolution == null) {
            resolution = this.resolution;
        }

        DateTime dateTime = new DateTime(time, timeZone);

        Node timeRoot = getTimeRoot();
        tx.acquireWriteLock(timeRoot);
        Node year = findOrCreateChild(timeRoot, dateTime.get(YEAR.getDateTimeFieldType()));
        return getInstant(year, dateTime, resolution);
    }

    @Override
    public Node getInstant(TimeInstant timeInstant, Transaction tx) {
        if (timeInstant.getTimezone() == null) {
            timeInstant.setTimezone(timeZone);
        }

        if (timeInstant.getResolution() == null) {
            timeInstant.setResolution(resolution);
        }

        DateTime dateTime = new DateTime(timeInstant.getTime(), timeInstant.getTimezone());

        Node timeRoot = getTimeRoot();
        tx.acquireWriteLock(timeRoot);
        Node year = findOrCreateChild(timeRoot, dateTime.get(YEAR.getDateTimeFieldType()));
        return getInstant(year, dateTime, timeInstant.getResolution());
    }


    @Override
    public List<Node> getInstants(TimeInstant startTime, TimeInstant endTime, Transaction tx) {

        if(startTime.getTime()>endTime.getTime()) {
            throw new IllegalArgumentException("Start time must be less than End time");
        }
        if (startTime.getTimezone() == null) {
            startTime.setTimezone(timeZone);
            if(endTime.getTimezone()!=null && (!startTime.getTimezone().equals(endTime.getTimezone()))) {
                throw new IllegalArgumentException("The timezone of startTime and endTime must match");
            }
        }

        if (startTime.getResolution() == null) {
            startTime.setResolution(resolution);
            if(endTime.getResolution()!=null && (!startTime.getResolution().equals(endTime.getResolution()))) {
                throw new IllegalArgumentException("The resolution of startTime and endTime must match");
            }
        }


        List<Node> result = new LinkedList<>();

        MutableDateTime time = new MutableDateTime(startTime.getTime());
        while (!time.isAfter(endTime.getTime())) {
            result.add(getInstant(time.getMillis(), startTime.getTimezone(), startTime.getResolution(), tx));
            time.add(startTime.getResolution().getDateTimeFieldType().getDurationType(), 1);
        }

        return result;
    }

    @Override
    public void attachEventToInstant(Node eventNode, RelationshipType eventRelation, Direction eventRelationDirection, TimeInstant timeInstant, Transaction tx) {
        Node timeInstantNode=getInstant(timeInstant,tx);
        if(eventRelationDirection.equals(Direction.OUTGOING)) {
            eventNode.createRelationshipTo(timeInstantNode,eventRelation);
        }
        else {
            timeInstantNode.createRelationshipTo(eventNode,eventRelation);
        }
    }

    @Override
    public List<Node> getEventsAtInstant(TimeInstant timeInstant, Transaction tx) {
        List<Node> events=new ArrayList<>();
        List<String> timeTreeRelationships = TimeTreeRelationshipTypes.getTimeTreeRelationshipNames();
        Node timeInstantNode=getInstant(timeInstant,tx);
        for(Relationship rel : timeInstantNode.getRelationships()) {
            if(!timeTreeRelationships.contains(rel.getType().name())) {
                events.add(rel.getOtherNode(timeInstantNode));
            }
        }
        return events;
    }

    @Override
    public List<Node> getEventsBetweenInstants(TimeInstant startTime, TimeInstant endTime, Transaction tx) {
        List<Node> events=new ArrayList<>();
        List<String> timeTreeRelationships = TimeTreeRelationshipTypes.getTimeTreeRelationshipNames();

        List<Node> timeInstants = getInstants(startTime, endTime, tx);
        for(Node timeInstantNode : timeInstants) {
            for (Relationship rel : timeInstantNode.getRelationships()) {
                if (!timeTreeRelationships.contains(rel.getType().name())) {
                    events.add(rel.getOtherNode(timeInstantNode));
                }
            }
        }
        return events;
    }

    /**
     * Get a node representing a specific time instant. If one doesn't exist, it will be created as well as any missing
     * nodes on the way down from parent (recursively).
     *
     * @param parent           parent node on path to desired instant node.
     * @param dateTime         time instant.
     * @param targetResolution target child resolution. Recursion stops when at this level.
     * @return node representing the time instant at the desired resolution level.
     */
    private Node getInstant(Node parent, DateTime dateTime, Resolution targetResolution) {
        Resolution currentResolution = findForNode(parent);

        if (currentResolution.equals(targetResolution)) {
            return parent;
        }

        Node child = findOrCreateChild(parent, dateTime.get(currentResolution.getChild().getDateTimeFieldType()));

        //recursion
        return getInstant(child, dateTime, targetResolution);
    }

    /**
     * Get the root of the time tree. Create it if it does not exist.
     *
     * @return root of the time tree.
     */
    protected Node getTimeRoot() {
        if (timeTreeRoot != null) {
            return timeTreeRoot;
        }

        synchronized (this) {
            if (timeTreeRoot != null) {
                return timeTreeRoot;
            }

            Node result;

            Iterator<Node> nodeIterator = GlobalGraphOperations.at(database).getAllNodesWithLabel(TimeTreeRoot).iterator();
            if (nodeIterator.hasNext()) {
                result = nodeIterator.next();
                if (nodeIterator.hasNext()) {
                    LOG.error("There is more than one time tree root!");
                    throw new IllegalStateException("There is more than one time tree root!");
                }
                return result;
            }

            LOG.info("Creating time tree root");

            timeTreeRoot = database.createNode(TimeTreeRoot);
            return timeTreeRoot;
        }
    }

    /**
     * Find a child node with value equal to the given value. If no such child exists, create one.
     *
     * @param parent parent of the node to be found or created.
     * @param value  value of the node to be found or created.
     * @return child node.
     */
    private Node findOrCreateChild(Node parent, int value) {
        Relationship firstRelationship = parent.getSingleRelationship(FIRST, OUTGOING);
        if (firstRelationship == null) {
            return createFirstChildEver(parent, value);
        }

        Node existingChild = firstRelationship.getEndNode();
        boolean isFirst = true;
        while ((int) existingChild.getProperty(VALUE_PROPERTY) < value && parent(existingChild).getId() == parent.getId()) {
            isFirst = false;
            Relationship nextRelationship = existingChild.getSingleRelationship(NEXT, OUTGOING);

            if (nextRelationship == null || parent(nextRelationship.getEndNode()).getId() != parent.getId()) {
                return createLastChild(parent, existingChild, nextRelationship == null ? null : nextRelationship.getEndNode(), value);
            }

            existingChild = nextRelationship.getEndNode();
        }

        if (existingChild.getProperty(VALUE_PROPERTY).equals(value)) {
            return existingChild;
        }

        Relationship previousRelationship = existingChild.getSingleRelationship(NEXT, INCOMING);

        if (isFirst) {
            return createFirstChild(parent, previousRelationship == null ? null : previousRelationship.getStartNode(), existingChild, value);
        }

        return createChild(parent, previousRelationship.getStartNode(), existingChild, value);
    }

    /**
     * Create the first ever child of a parent.
     *
     * @param parent to create child for.
     * @param value  value of the node to be created.
     * @return child node.
     */
    private Node createFirstChildEver(Node parent, int value) {
        if (parent.getSingleRelationship(LAST, OUTGOING) != null) { //sanity check
            LOG.error(parent.toString() + " has no " + FIRST.name() + " relationship, but has a " + LAST.name() + " one!");
            throw new IllegalStateException(parent.toString() + " has no " + FIRST.name() + " relationship, but has a " + LAST.name() + " one!");
        }

        Node previousChild = null;
        Relationship previousParentRelationship = parent.getSingleRelationship(NEXT, INCOMING);
        if (previousParentRelationship != null) {
            Relationship previousParentLastChildRelationship = previousParentRelationship.getStartNode().getSingleRelationship(LAST, OUTGOING);
            if (previousParentLastChildRelationship != null) {
                previousChild = previousParentLastChildRelationship.getEndNode();
            }
        }

        Node nextChild = null;
        Relationship nextParentRelationship = parent.getSingleRelationship(NEXT, OUTGOING);
        if (nextParentRelationship != null) {
            Relationship nextParentFirstChildRelationship = nextParentRelationship.getEndNode().getSingleRelationship(FIRST, OUTGOING);
            if (nextParentFirstChildRelationship != null) {
                nextChild = nextParentFirstChildRelationship.getEndNode();
            }
        }

        Node child = createChild(parent, previousChild, nextChild, value);

        parent.createRelationshipTo(child, FIRST);
        parent.createRelationshipTo(child, LAST);

        return child;
    }

    /**
     * Create the first child node that belongs to a specific parent. "First" is with respect to ordering, not the
     * number of nodes. In other words, the node being created is not the first parent's child, but it is the child with
     * the lowest ordering.
     *
     * @param parent        to create child for.
     * @param previousChild previous child (has different parent), or null for no such child.
     * @param nextChild     next child (has same parent).
     * @param value         value of the node to be created.
     * @return child node.
     */
    private Node createFirstChild(Node parent, Node previousChild, Node nextChild, int value) {
        Relationship firstRelationship = parent.getSingleRelationship(FIRST, OUTGOING);

        if (nextChild.getId() != firstRelationship.getEndNode().getId()) { //sanity check
            LOG.error(nextChild.toString() + " seems to be the first child of node " + parent.toString() + ", but there is no " + FIRST.name() + " relationship between the two!");
            throw new IllegalStateException(nextChild.toString() + " seems to be the first child of node " + parent.toString() + ", but there is no " + FIRST.name() + " relationship between the two!");
        }

        firstRelationship.delete();

        Node child = createChild(parent, previousChild, nextChild, value);

        parent.createRelationshipTo(child, FIRST);

        return child;
    }

    /**
     * Create the last child node that belongs to a specific parent.
     *
     * @param parent        to create child for.
     * @param previousChild previous child (has same parent).
     * @param nextChild     next child (has different parent), or null for no such child.
     * @param value         value of the node to be created.
     * @return child node.
     */
    private Node createLastChild(Node parent, Node previousChild, Node nextChild, int value) {
        Relationship lastRelationship = parent.getSingleRelationship(LAST, OUTGOING);

        Node endNode = lastRelationship.getEndNode();
        if (previousChild.getId() != endNode.getId()) { //sanity check
            LOG.error(previousChild.toString() + " seems to be the last child of node " + parent.toString() + ", but there is no " + LAST.name() + " relationship between the two!");
            throw new IllegalStateException(previousChild.toString() + " seems to be the last child of node " + parent.toString() + ", but there is no " + LAST.name() + " relationship between the two!");
        }

        lastRelationship.delete();

        Node child = createChild(parent, previousChild, nextChild, value);

        parent.createRelationshipTo(child, LAST);

        return child;
    }

    /**
     * Create a child node.
     *
     * @param parent   parent node.
     * @param previous previous node on the same level, null if the child is the first one.
     * @param next     next node on the same level, null if the child is the last one.
     * @param value    value of the child.
     * @return the newly created child.
     */
    private Node createChild(Node parent, Node previous, Node next, int value) {
        if (previous != null && next != null && next.getId() != previous.getSingleRelationship(NEXT, OUTGOING).getEndNode().getId()) {
            LOG.error(previous.toString() + " and " + next.toString() + " are not connected with a " + NEXT.name() + " relationship!");
            throw new IllegalArgumentException(previous.toString() + " and " + next.toString() + " are not connected with a " + NEXT.name() + " relationship!");
        }

        Node child = database.createNode(TimeTreeLabels.getChild(parent));
        child.setProperty(VALUE_PROPERTY, value);
        parent.createRelationshipTo(child, CHILD);

        if (previous != null) {
            Relationship nextRelationship = previous.getSingleRelationship(NEXT, OUTGOING);
            if (nextRelationship != null) {
                nextRelationship.delete();
            }
            previous.createRelationshipTo(child, NEXT);
        }

        if (next != null) {
            child.createRelationshipTo(next, NEXT);
        }

        return child;
    }

    /**
     * Find the parent of a node.
     *
     * @param node to find a parent for.
     * @return parent.
     * @throws IllegalStateException in case the node has no parent.
     */
    private Node parent(Node node) {
        Relationship parentRelationship = node.getSingleRelationship(CHILD, INCOMING);

        if (parentRelationship == null) {
            LOG.error(node.toString() + " has no parent!");
            throw new IllegalStateException(node.toString() + " has no parent!");
        }

        return parentRelationship.getStartNode();
    }
}
