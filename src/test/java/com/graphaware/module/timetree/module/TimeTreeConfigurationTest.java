/*
 * Copyright (c) 2013-2020 GraphAware
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

import com.graphaware.common.policy.inclusion.composite.CompositeNodeInclusionPolicy;
import com.graphaware.common.policy.inclusion.spel.SpelNodeInclusionPolicy;
import org.junit.Test;
import org.neo4j.graphdb.RelationshipType;

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
                .withRelationshipType(RelationshipType.withName("TEST"))
                .with(CompositeNodeInclusionPolicy.of(new SpelNodeInclusionPolicy("hasLabel('Test')")));

        TimeTreeConfiguration c2 = TimeTreeConfiguration.defaultConfiguration()
                .withAutoAttach(true)
                .withTimestampProperty("created")
                .withRelationshipType(RelationshipType.withName("TEST"))
                .with(CompositeNodeInclusionPolicy.of(new SpelNodeInclusionPolicy("hasLabel('Test')")));

        assertTrue(c1.equals(c2));
        assertTrue(c2.equals(c1));
    }
}
