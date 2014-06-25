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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

/**
 * {@link org.neo4j.graphdb.Label}s for {@link TimeTree}.
 */
public enum TimeTreeLabels implements Label
{

    TimeTreeRoot, Year, Month, Day, Hour, Minute, Second, Millisecond;

    private static final Logger LOG = LoggerFactory.getLogger(TimeTreeLabels.class);

    /**
     * Get the label representing a resolution one level lower than this label.
     *
     * @return child label.
     * @throws IllegalStateException if there is no child label.
     */
    public Label getChild() {
        if (this.ordinal() >= values().length - 1) {
            LOG.error("Label " + this.toString() + " does not have children. This is a bug.");
            throw new IllegalArgumentException("Label " + this.toString() + " does not have children. This is a bug.");
        }

        return values()[this.ordinal() + 1];
    }

    /**
     * Get the label representing a resolution one level lower than represented by the given node. If the node isn't
     * from GraphAware time tree, {@link #Year} is returned by default.
     *
     * @param node to find child label for.
     * @return child label.
     */
    public static Label getChild(Node node) {
        for (Label label : node.getLabels()) {
            try {
                return TimeTreeLabels.valueOf(label.name()).getChild();
            } catch (IllegalArgumentException e) {
                //ok
            }
        }

        return Year;
    }
}
