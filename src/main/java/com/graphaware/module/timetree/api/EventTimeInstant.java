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

import com.graphaware.module.timetree.Event;

/**
 * Representation of an event that occurs at a time instant
 */
public class EventTimeInstant {

    private long eventNodeId;
    private String eventRelationshipType;
    private String eventRelationshipDirection;
    private String timezone;
    private String resolution;
    private long time;

    public EventTimeInstant() {
    }

    public EventTimeInstant(Event event) {
        eventNodeId = event.getEventNode().getId();
        eventRelationshipType = event.getEventRelation().name();
        timezone = event.getTimeInstant().getTimezone().getID();
        resolution = event.getTimeInstant().getResolution().name();
        time = event.getTimeInstant().getTime();
    }


    public long getEventNodeId() {
        return eventNodeId;
    }

    public void setEventNodeId(long eventNodeId) {
        this.eventNodeId = eventNodeId;
    }

    public String getEventRelationshipType() {
        return eventRelationshipType;
    }

    public void setEventRelationshipType(String eventRelationshipType) {
        this.eventRelationshipType = eventRelationshipType;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getEventRelationshipDirection() {
        return eventRelationshipDirection;
    }

    public void setEventRelationshipDirection(String eventRelationshipDirection) {
        this.eventRelationshipDirection = eventRelationshipDirection;
    }
}
