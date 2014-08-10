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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * An instant of time
 */
public class TimeInstant {

    private DateTimeZone timezone;
    private Resolution resolution;
    private long time;

    public TimeInstant() {
        time = DateTime.now().getMillis();
    }

    public TimeInstant(long time) {
        this.time = time;
    }

    /**
     * Get the timezone set for this TimeInstant
     *
     * @return the timezone
     */
    public DateTimeZone getTimezone() {
        return timezone;
    }

    /**
     * Set the timezone for this TimeInstant
     *
     * @param timezone the timezone
     */
    public void setTimezone(DateTimeZone timezone) {
        this.timezone = timezone;
    }

    /**
     * Get the {@link com.graphaware.module.timetree.Resolution} set for this TimeInstant
     *
     * @return the resolution
     */
    public Resolution getResolution() {
        return resolution;
    }

    /**
     * Set the {@link com.graphaware.module.timetree.Resolution} for this TimeInstant
     *
     * @param resolution the resolution
     */
    public void setResolution(Resolution resolution) {
        this.resolution = resolution;
    }

    /**
     * Get the UTC time in ms from 1/1/1970 for this TimeInstant
     *
     * @return the UTC time in ms from 1/1/1970.
     */
    public long getTime() {
        return time;
    }

    /**
     * Set the UTC time in ms from 1/1/1970 for this TimeInstant
     *
     * @param time the UTC time in ms from 1/1/1970.
     */
    public void setTime(long time) {
        this.time = time;
    }
}
