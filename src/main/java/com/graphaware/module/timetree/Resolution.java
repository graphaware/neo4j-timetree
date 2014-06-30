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
import org.joda.time.DateTimeFieldType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

/**
 * Resolution of a {@link TimeTree}.
 */
public enum Resolution {

    YEAR(TimeTreeLabels.Year, DateTimeFieldType.year()),
    MONTH(TimeTreeLabels.Month, DateTimeFieldType.monthOfYear()),
    DAY(TimeTreeLabels.Day, DateTimeFieldType.dayOfMonth()),
    HOUR(TimeTreeLabels.Hour, DateTimeFieldType.hourOfDay()),
    MINUTE(TimeTreeLabels.Minute, DateTimeFieldType.minuteOfHour()),
    SECOND(TimeTreeLabels.Second, DateTimeFieldType.secondOfMinute()),
    MILLISECOND(TimeTreeLabels.Millisecond, DateTimeFieldType.millisOfSecond());

    private static final Logger LOG = LoggerFactory.getLogger(Resolution.class);

    private final Label label;
    private final DateTimeFieldType dateTimeFieldType;

    private Resolution(Label label, DateTimeFieldType dateTimeFieldType) {
        this.label = label;
        this.dateTimeFieldType = dateTimeFieldType;
    }

    /**
     * Get the label corresponding to this resolution level. Nodes representing the level will get this label.
     *
     * @return label.
     */
    public Label getLabel() {
        return label;
    }

    /**
     * Get the {@link DateTimeFieldType} corresponding to this resolution level.
     *
     * @return field type.
     */
    public DateTimeFieldType getDateTimeFieldType() {
        return dateTimeFieldType;
    }

    /**
     * Get the resolution one level below this resolution.
     *
     * @return child resolution.
     * @throws IllegalStateException if this resolution does not have children.
     */
    public Resolution getChild() {
        if (this.ordinal() >= values().length - 1) {
            LOG.error("Parent resolution " + this.toString() + " does not have children. This is a bug.");
            throw new IllegalStateException("Parent resolution " + this.toString() + " does not have children. This is a bug.");
        }

        return values()[this.ordinal() + 1];
    }

    /**
     * Find the resolution level that the given node corresponds to. The node must be from a GraphAware TimeTree and must
     * not be the root of the tree.
     *
     * @param node to find resolution for.
     * @return resolution.
     * @throws IllegalArgumentException in case the given node is not from GraphAware TimeTree or is the root.
     */
    public static Resolution findForNode(Node node) {
        for (Label label : node.getLabels()) {
            Resolution resolution = findForLabel(label);
            if (resolution != null) {
                return resolution;
            }
        }

        LOG.error("Node " + node.toString() + " does not have a corresponding resolution. This is a bug.");
        throw new IllegalArgumentException("Node " + node.toString() + " does not have a corresponding resolution. This is a bug.");
    }

    /**
     * Find the resolution corresponding to the given label.
     *
     * @param label to find the resolution for.
     * @return resolution for label, null if there is no corresponding resolution.
     */
    private static Resolution findForLabel(Label label) {
        for (Resolution resolution : values()) {
            if (resolution.getLabel().name().equals(label.name())) {
                return resolution;
            }
        }

        return null;
    }
}
