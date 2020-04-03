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

import com.graphaware.common.policy.inclusion.InclusionPolicies;
import com.graphaware.common.policy.inclusion.fluent.IncludeNodes;
import com.graphaware.common.policy.inclusion.fluent.IncludeRelationships;
import com.graphaware.module.timetree.domain.Resolution;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;
import com.graphaware.runtime.policy.InclusionPoliciesFactory;
import org.joda.time.DateTimeZone;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

import java.util.TimeZone;

import static com.graphaware.module.timetree.domain.Resolution.DAY;


/**
 * {@link com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration} for {@link com.graphaware.module.timetree.module.TimeTreeModule}.
 */
public class TimeTreeConfiguration extends BaseTxDrivenModuleConfiguration<TimeTreeConfiguration> {

    private static final Resolution DEFAULT_RESOLUTION = DAY;
    private static final DateTimeZone DEFAULT_TIME_ZONE = DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"));
    private static final String DEFAULT_TIMESTAMP_PROPERTY = "timestamp";
    private static final String DEFAULT_CUSTOM_TIMETREE_ROOT_PROPERTY = "timeTreeRootId";
    private static final RelationshipType DEFAULT_RELATIONSHIP_TYPE = RelationshipType.withName("AT_TIME");
    private static final Direction DEFAULT_DIRECTION = Direction.INCOMING;
    private static final boolean DEFAULT_AUTO_ATTACH = false;

    private static final InclusionPolicies DEFAULT_INCLUSION_POLICIES =
            InclusionPoliciesFactory.allBusiness()
                    .with(IncludeNodes.all().with("Event"))
                    .with(IncludeRelationships.all().with(DEFAULT_RELATIONSHIP_TYPE));

    private final String timestampProperty;
    private final String customTimeTreeRootProperty;
    private final Resolution resolution;
    private final DateTimeZone timeZone;
    private final String relationshipType;
    private final Direction direction;
    private final boolean autoAttach;

    /**
     * Create a new configuration.
     *
     * @param inclusionPolicies          for the event nodes. Only {@link com.graphaware.common.policy.inclusion.NodeInclusionPolicy} and
     *                                   {@link com.graphaware.common.policy.inclusion.NodePropertyInclusionPolicy} are interesting. The {@link com.graphaware.common.policy.inclusion.NodeInclusionPolicy}
     *                                   should include the nodes that should be attached to the tree, and the {@link com.graphaware.common.policy.inclusion.NodePropertyInclusionPolicy}
     *                                   must include the timestamp property of those nodes.
     * @param initializeUntil            until what time in ms since epoch it is ok to re(initialize) the entire module in case the configuration
     *                                   has changed since the last time the module was started, or if it is the first time the module was registered.
     *                                   {@link #NEVER} for never, {@link #ALWAYS} for always.
     * @param timestampProperty          property of the event nodes that stores a <code>long</code> timestamp.
     * @param customTimeTreeRootProperty property of the event nodes that stores a <code>long</code> representing their custom timetree root node ID
     * @param resolution                 resolution of the tree, to which to attach events.
     * @param timeZone                   time zone which is used for representing timestamps in the tree.
     * @param relationshipType           with which the events are attached to the tree.
     * @param direction                  with which the events are attached to the tree (from the tree's point of view).
     * @param autoAttach                 <code>true</code> iff events should be automatically attached upon first module run and when config changes.
     */
    protected TimeTreeConfiguration(InclusionPolicies inclusionPolicies, long initializeUntil, String timestampProperty, String customTimeTreeRootProperty, Resolution resolution, DateTimeZone timeZone, RelationshipType relationshipType, Direction direction, boolean autoAttach) {
        super(inclusionPolicies, initializeUntil);
        this.timestampProperty = timestampProperty;
        this.customTimeTreeRootProperty = customTimeTreeRootProperty;
        this.resolution = resolution;
        this.timeZone = timeZone;
        this.relationshipType = relationshipType.name();
        this.direction = direction;
        this.autoAttach = autoAttach;
    }

    /**
     * Create a default configuration with
     * default inclusion policies = {@link #DEFAULT_INCLUSION_POLICIES} (node labelled Event with a property called timestamp),
     * default timestamp property = {@link #DEFAULT_TIMESTAMP_PROPERTY},
     * default customTimeTree root property = {@link #DEFAULT_CUSTOM_TIMETREE_ROOT_PROPERTY},
     * default resolution = {@link #DEFAULT_RESOLUTION},
     * default time zone = {@link #DEFAULT_TIME_ZONE}, and
     * default relationship type = {@link #DEFAULT_RELATIONSHIP_TYPE}
     * <p>
     * Change the configuration by using the fluent with* methods.
     *
     * @return default config.
     */
    public static TimeTreeConfiguration defaultConfiguration() {
        return new TimeTreeConfiguration(DEFAULT_INCLUSION_POLICIES, ALWAYS, DEFAULT_TIMESTAMP_PROPERTY, DEFAULT_CUSTOM_TIMETREE_ROOT_PROPERTY, DEFAULT_RESOLUTION, DEFAULT_TIME_ZONE, DEFAULT_RELATIONSHIP_TYPE, DEFAULT_DIRECTION, DEFAULT_AUTO_ATTACH);
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different timestamp property.
     *
     * @param timestampProperty of the new instance.
     * @return new instance.
     */
    public TimeTreeConfiguration withTimestampProperty(final String timestampProperty) {
        return new TimeTreeConfiguration(getInclusionPolicies(), initializeUntil(), timestampProperty, getCustomTimeTreeRootProperty(), getResolution(), getTimeZone(), getRelationshipType(), getDirection(), isAutoAttach());
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different customTimeTreeRootID property
     *
     * @param customTimeTreeRootProperty
     * @return new instance
     */
    public TimeTreeConfiguration withCustomTimeTreeRootProperty(final String customTimeTreeRootProperty) {
        return new TimeTreeConfiguration(getInclusionPolicies(), initializeUntil(), getTimestampProperty(), customTimeTreeRootProperty, getResolution(), getTimeZone(), getRelationshipType(), getDirection(), isAutoAttach());
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different resolution.
     *
     * @param resolution of the new instance.
     * @return new instance.
     */
    public TimeTreeConfiguration withResolution(Resolution resolution) {
        return new TimeTreeConfiguration(getInclusionPolicies(), initializeUntil(), getTimestampProperty(), getCustomTimeTreeRootProperty(), resolution, getTimeZone(), getRelationshipType(), getDirection(), isAutoAttach());
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different time zone.
     *
     * @param timeZone of the new instance.
     * @return new instance.
     */
    public TimeTreeConfiguration withTimeZone(DateTimeZone timeZone) {
        return new TimeTreeConfiguration(getInclusionPolicies(), initializeUntil(), getTimestampProperty(), getCustomTimeTreeRootProperty(), getResolution(), timeZone, getRelationshipType(), getDirection(), isAutoAttach());
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different relationship type.
     *
     * @param relationshipType of the new instance.
     * @return new instance.
     */
    public TimeTreeConfiguration withRelationshipType(final RelationshipType relationshipType) {
        return new TimeTreeConfiguration(getInclusionPolicies().with(IncludeRelationships.all().with(relationshipType)), initializeUntil(), getTimestampProperty(), getCustomTimeTreeRootProperty(), getResolution(), getTimeZone(), relationshipType, getDirection(), isAutoAttach());
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different direction.
     *
     * @param direction of the new instance.
     * @return new instance.
     */
    public TimeTreeConfiguration withDirection(final Direction direction) {
        if (!Direction.INCOMING.equals(direction) && !Direction.OUTGOING.equals(direction)) {
            throw new IllegalArgumentException("Direction must be INCOMING or OUTGOING!");
        }
        return new TimeTreeConfiguration(getInclusionPolicies().with(IncludeRelationships.all().with(relationshipType)), initializeUntil(), getTimestampProperty(), getCustomTimeTreeRootProperty(), getResolution(), getTimeZone(), getRelationshipType(), direction, isAutoAttach());
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different setting for automatic attachment.
     *
     * @param autoAttach of the new instance.
     * @return new instance.
     */
    public TimeTreeConfiguration withAutoAttach(final boolean autoAttach) {
        return new TimeTreeConfiguration(getInclusionPolicies(), initializeUntil(), getTimestampProperty(), getCustomTimeTreeRootProperty(), getResolution(), getTimeZone(), getRelationshipType(), getDirection(), autoAttach);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TimeTreeConfiguration newInstance(InclusionPolicies inclusionPolicies, long initializeUntil) {
        return new TimeTreeConfiguration(inclusionPolicies
                .with(IncludeRelationships.all().with(getRelationshipType())),
                initializeUntil(), getTimestampProperty(), getCustomTimeTreeRootProperty(), getResolution(), getTimeZone(), getRelationshipType(), getDirection(), isAutoAttach());
    }

    public String getTimestampProperty() {
        return timestampProperty;
    }

    public String getCustomTimeTreeRootProperty() {
        return customTimeTreeRootProperty;
    }

    public Resolution getResolution() {
        return resolution;
    }

    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    public RelationshipType getRelationshipType() {
        return RelationshipType.withName(relationshipType);
    }

    public Direction getDirection() {
        return direction;
    }

    public boolean isAutoAttach() {
        return autoAttach;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        TimeTreeConfiguration that = (TimeTreeConfiguration) o;

        if (autoAttach != that.autoAttach) {
            return false;
        }
        if (!relationshipType.equals(that.relationshipType)) {
            return false;
        }
        if (!direction.name().equals(that.direction.name())) {
            return false;
        }
        if (resolution != that.resolution) {
            return false;
        }
        if (!timeZone.equals(that.timeZone)) {
            return false;
        }
        if (!timestampProperty.equals(that.timestampProperty)) {
            return false;
        }
        if (!customTimeTreeRootProperty.equals(that.customTimeTreeRootProperty)) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + timestampProperty.hashCode();
        result = 31 * result + customTimeTreeRootProperty.hashCode();
        result = 31 * result + resolution.hashCode();
        result = 31 * result + timeZone.hashCode();
        result = 31 * result + relationshipType.hashCode();
        result = 31 * result + direction.name().hashCode();
        result = 31 * result + (autoAttach ? 1 : 0);
        return result;
    }
}
