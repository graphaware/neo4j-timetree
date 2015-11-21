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

package com.graphaware.module.timetree.module;

import com.graphaware.common.policy.BaseNodeInclusionPolicy;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.Iterables;

public final class IncludeEvents extends BaseNodeInclusionPolicy {

    private static final Label EVENT = DynamicLabel.label("Event");

    private static final IncludeEvents INSTANCE = new IncludeEvents();

    public static IncludeEvents getInstance() {
        return INSTANCE;
    }

    private IncludeEvents() {
    }

    @Override
    public boolean include(Node node) {
        return node.hasLabel(EVENT);
    }

    @Override
    protected Iterable<Node> doGetAll(GraphDatabaseService database) {
        return Iterables.asResourceIterable(database.findNodes(EVENT));
    }
}
