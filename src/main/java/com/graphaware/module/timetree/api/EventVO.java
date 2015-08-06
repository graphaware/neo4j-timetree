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

import com.graphaware.api.JsonNode;

/**
 * Representation of an event.
 */
public class EventVO {

    private JsonNode node;
    private String relationshipType;

    public EventVO() {
    }

    public EventVO(JsonNode node, String relationshipType) {
        this.node = node;
        this.relationshipType = relationshipType;
    }

    public JsonNode getNode() {
        return node;
    }

    public void setNode(JsonNode node) {
        this.node = node;
    }

    public String getRelationshipType() {
        return relationshipType;
    }

    public void setRelationshipType(String relationshipType) {
        this.relationshipType = relationshipType;
    }

    public void validate() {
        if (node == null) {
            throw new IllegalArgumentException("Event node must be specified");
        }
        if (relationshipType == null) {
            throw new IllegalArgumentException("Relationship type for event must not be null");
        }
    }
}
