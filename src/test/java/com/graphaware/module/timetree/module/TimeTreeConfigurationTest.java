/*
 * Copyright (c) 2015 GraphAware
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

package com.graphaware.module.timetree.module;

import com.graphaware.common.policy.composite.CompositeNodeInclusionPolicy;
import com.graphaware.common.policy.spel.SpelNodeInclusionPolicy;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link com.graphaware.module.timetree.module.TimeTreeConfiguration}
 */
public class TimeTreeConfigurationTest {

    @Test //https://github.com/graphaware/neo4j-timetree/issues/28
    public void sameConfigsShouldBeEqual() {
        TimeTreeConfiguration c1 = TimeTreeConfiguration.defaultConfiguration()
                .withAutoAttach(true)
                .withTimestampProperty("created")
                .withRelationshipType(DynamicRelationshipType.withName("TEST"))
                .withRelationshipDirection(Direction.INCOMING)
                .with(CompositeNodeInclusionPolicy.of(new SpelNodeInclusionPolicy("hasLabel('Test')")));

        TimeTreeConfiguration c2 = TimeTreeConfiguration.defaultConfiguration()
                .withAutoAttach(true)
                .withTimestampProperty("created")
                .withRelationshipType(DynamicRelationshipType.withName("TEST"))
                .withRelationshipDirection(Direction.INCOMING)
                .with(CompositeNodeInclusionPolicy.of(new SpelNodeInclusionPolicy("hasLabel('Test')")));

        assertTrue(c1.equals(c2));
        assertTrue(c2.equals(c1));
    }
}
