/*
 * Copyright (c) 2013 GraphAware
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
package com.graphaware.module.timetree.api;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

/**
 * Representation of an event that occurs at a time instant
 */
public class EventVO {

    private long nodeId;
    @JsonUnwrapped
    private TimeInstantVO timeInstant;
    private String relationshipType;

    public EventVO() {
    }

    public EventVO(long nodeId, TimeInstantVO timeInstant, String relationshipType) {
        this.nodeId = nodeId;
        this.timeInstant = timeInstant;
        this.relationshipType = relationshipType;
    }

    public long getNodeId() {
        return nodeId;
    }

    public void setNodeId(long nodeId) {
        this.nodeId = nodeId;
    }

    public TimeInstantVO getTimeInstant() {
        return timeInstant;
    }

    public void setTimeInstant(TimeInstantVO timeInstant) {
        this.timeInstant = timeInstant;
    }

    public String getRelationshipType() {
        return relationshipType;
    }

    public void setRelationshipType(String relationshipType) {
        this.relationshipType = relationshipType;
    }

    public void validate() {
        if (timeInstant == null) {
            throw new IllegalArgumentException("Time instant for Event must not be null");
        }

        if (relationshipType == null) {
            throw new IllegalArgumentException("Relationship type for Event must not be null");
        }
    }
}
