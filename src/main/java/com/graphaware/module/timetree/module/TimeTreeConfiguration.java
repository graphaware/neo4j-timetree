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

import com.graphaware.common.policy.InclusionPolicies;
import com.graphaware.common.policy.fluent.IncludeNodeProperties;
import com.graphaware.common.policy.fluent.IncludeNodes;
import com.graphaware.common.policy.fluent.IncludeRelationships;
import com.graphaware.module.timetree.domain.Resolution;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;
import com.graphaware.runtime.policy.InclusionPoliciesFactory;
import org.joda.time.DateTimeZone;
import org.neo4j.graphdb.DynamicRelationshipType;
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
    private static final RelationshipType DEFAULT_RELATIONSHIP_TYPE = DynamicRelationshipType.withName("AT_TIME");
    private static final boolean DEFAULT_AUTO_ATTACH = false;

    private static final InclusionPolicies DEFAULT_INCLUSION_POLICIES =
            InclusionPoliciesFactory.allBusiness()
                    .with(IncludeNodes.all().with("Event"))
                    .with(IncludeNodeProperties.all().with(DEFAULT_TIMESTAMP_PROPERTY))
                    .with(IncludeNodeProperties.all().with(DEFAULT_CUSTOM_TIMETREE_ROOT_PROPERTY))
                    .with(IncludeRelationships.all().with(DEFAULT_RELATIONSHIP_TYPE));

    private String timestampProperty;
    private String customTimeTreeRootProperty;
    private Resolution resolution;
    private DateTimeZone timeZone;
    private RelationshipType relationshipType;
    private boolean autoAttach;

    /**
     * Create a new configuration.
     *
     * @param inclusionPolicies  for the event nodes. Only {@link com.graphaware.common.policy.NodeInclusionPolicy} and
     *                           {@link com.graphaware.common.policy.NodePropertyInclusionPolicy} are interesting. The {@link com.graphaware.common.policy.NodeInclusionPolicy}
     *                           should include the nodes that should be attached to the tree, and the {@link com.graphaware.common.policy.NodePropertyInclusionPolicy}
     *                           must include the timestamp property of those nodes.
     * @param timestampProperty  property of the event nodes that stores a <code>long</code> timestamp.
     * @param CustomTimeTreeRootProperty property of the event nodes that stores a <code>long</code> representing their custom timetree root
     * @param resolution         resolution of the tree, to which to attach events.
     * @param timeZone           time zone which is used for representing timestamps in the tree.
     * @param relationshipType   with which the events are attached to the tree.
     * @param autoAttach         <code>true</code> iff events should be automatically attached upon first module run and when config changes.
     */
    protected TimeTreeConfiguration(InclusionPolicies inclusionPolicies, String timestampProperty, String CustomTimeTreeRootProperty, Resolution resolution, DateTimeZone timeZone, RelationshipType relationshipType, boolean autoAttach) {
        super(inclusionPolicies);
        this.timestampProperty = timestampProperty;
        this.customTimeTreeRootProperty = CustomTimeTreeRootProperty;
        this.resolution = resolution;
        this.timeZone = timeZone;
        this.relationshipType = relationshipType;
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
     * <p/>
     * Change the configuration by using the fluent with* methods.
     *
     * @return default config.
     */
    public static TimeTreeConfiguration defaultConfiguration() {
        return new TimeTreeConfiguration(DEFAULT_INCLUSION_POLICIES, DEFAULT_TIMESTAMP_PROPERTY, DEFAULT_CUSTOM_TIMETREE_ROOT_PROPERTY, DEFAULT_RESOLUTION, DEFAULT_TIME_ZONE, DEFAULT_RELATIONSHIP_TYPE, DEFAULT_AUTO_ATTACH);
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different timestamp property.
     *
     * @param timestampProperty of the new instance.
     * @return new instance.
     */
    public TimeTreeConfiguration withTimestampProperty(final String timestampProperty) {
        return new TimeTreeConfiguration(getInclusionPolicies().with(IncludeNodeProperties.all().with(timestampProperty)), timestampProperty, getCustomTimeTreeRootProperty(), getResolution(), getTimeZone(), getRelationshipType(), isAutoAttach());
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different customTimeTreeRootID property
     *
     * @param CustomTimeTreeRootProperty
     * @return new instance
     */
    public TimeTreeConfiguration withCustomTimeTreeRootProperty(final String CustomTimeTreeRootProperty) {
        return new TimeTreeConfiguration(getInclusionPolicies().with(IncludeNodeProperties.all().with(customTimeTreeRootProperty)), timestampProperty, getCustomTimeTreeRootProperty(), getResolution(), getTimeZone(), getRelationshipType(), isAutoAttach());
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different resolution.
     *
     * @param resolution of the new instance.
     * @return new instance.
     */
    public TimeTreeConfiguration withResolution(Resolution resolution) {
        return new TimeTreeConfiguration(getInclusionPolicies(), getTimestampProperty(), getCustomTimeTreeRootProperty(), resolution, getTimeZone(), getRelationshipType(), isAutoAttach());
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different time zone.
     *
     * @param timeZone of the new instance.
     * @return new instance.
     */
    public TimeTreeConfiguration withTimeZone(DateTimeZone timeZone) {
        return new TimeTreeConfiguration(getInclusionPolicies(), getTimestampProperty(), getCustomTimeTreeRootProperty(), getResolution(), timeZone, getRelationshipType(), isAutoAttach());
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different relationship type.
     *
     * @param relationshipType of the new instance.
     * @return new instance.
     */
    public TimeTreeConfiguration withRelationshipType(final RelationshipType relationshipType) {
        return new TimeTreeConfiguration(getInclusionPolicies().with(IncludeRelationships.all().with(relationshipType)), getTimestampProperty(), getCustomTimeTreeRootProperty(), getResolution(), getTimeZone(), relationshipType, isAutoAttach());
    }

    /**
     * Create a new instance of this {@link TimeTreeConfiguration} with different setting for automatic attachment.
     *
     * @param autoAttach of the new instance.
     * @return new instance.
     */
    public TimeTreeConfiguration withAutoAttach(final boolean autoAttach) {
        return new TimeTreeConfiguration(getInclusionPolicies(), getTimestampProperty(), getCustomTimeTreeRootProperty(), getResolution(), getTimeZone(), getRelationshipType(), autoAttach);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TimeTreeConfiguration newInstance(InclusionPolicies inclusionPolicies) {
        return new TimeTreeConfiguration(inclusionPolicies
                .with(IncludeNodeProperties.all().with(getTimestampProperty()))
                .with(IncludeNodeProperties.all().with(getCustomTimeTreeRootProperty()))
                .with(IncludeRelationships.all().with(getRelationshipType())),
                getTimestampProperty(), getCustomTimeTreeRootProperty(), getResolution(), getTimeZone(), getRelationshipType(), isAutoAttach());
    }

    public String getTimestampProperty() {
        return timestampProperty;
    }

    public String getCustomTimeTreeRootProperty() { return customTimeTreeRootProperty; }

    public Resolution getResolution() {
        return resolution;
    }

    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    public RelationshipType getRelationshipType() {
        return relationshipType;
    }

    public boolean isAutoAttach() {
        return autoAttach;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TimeTreeConfiguration that = (TimeTreeConfiguration) o;

        if (autoAttach != that.autoAttach) return false;
        if (!relationshipType.equals(that.relationshipType)) return false;
        if (resolution != that.resolution) return false;
        if (!timeZone.equals(that.timeZone)) return false;
        if (!timestampProperty.equals(that.timestampProperty)) return false;
        if (!customTimeTreeRootProperty.equals(that.customTimeTreeRootProperty)) return false;

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
        result = 31 * result + (autoAttach ? 1 : 0);
        return result;
    }
}
