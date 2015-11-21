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

import org.neo4j.graphdb.Node;

/**
 * An implementation of {@link TimeTree} which can have a custom time tree root provided to it. Thus, it allows for many
 * different time trees within a single graph.
 */
public class CustomRootTimeTree extends SingleTimeTree {

    private final Node root;

    public CustomRootTimeTree(Node root) {
        super(root.getGraphDatabase());
        this.root = root;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Node getTimeRoot() {
        return root;
    }
}
