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

import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link org.neo4j.graphdb.RelationshipType}s for {@link TimeTree}.
 */
public enum TimeTreeRelationshipTypes implements RelationshipType
{
    FIRST, LAST, NEXT, CHILD;

    public static List<String> getTimeTreeRelationshipNames() {
        List<String> relationNames=new ArrayList<>();
        for(TimeTreeRelationshipTypes type : values()) {
            relationNames.add(type.name());
        }
        return relationNames;
    }
}

