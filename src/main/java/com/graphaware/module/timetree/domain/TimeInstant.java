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
package com.graphaware.module.timetree.domain;

import com.graphaware.module.timetree.api.TimeInstantVO;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.neo4j.graphdb.Node;

import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import static com.graphaware.module.timetree.domain.Resolution.DAY;

/**
 * An instant of time.
 */
public class TimeInstant {

    private static final Resolution DEFAULT_RESOLUTION = DAY;
    private static final DateTimeZone DEFAULT_TIME_ZONE = DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"));

    private final long time;
    private final DateTimeZone timezone;
    private final Resolution resolution;

    /**
     * Create a new time instant representing now in UTC timezone with {@link Resolution#DAY}.
     */
    public static TimeInstant now() {
        return instant(DateTime.now().getMillis());
    }

    /**
     * Create a new time instant representing the given time in UTC timezone with {@link Resolution#DAY}.
     *
     * @param time to represent.
     */
    public static TimeInstant instant(long time) {
        return new TimeInstant(time, DEFAULT_TIME_ZONE, DEFAULT_RESOLUTION);
    }

    /**
     * Create a new time instant from this time instant with a different time zone.
     *
     * @param timezone of the new instant.
     * @return new instant.
     */
    public TimeInstant with(DateTimeZone timezone) {
        return new TimeInstant(getTime(), timezone, getResolution());
    }

    /**
     * Create a new time instant from this time instant with a different resolution.
     *
     * @param resolution of the new instant.
     * @return new instant.
     */
    public TimeInstant with(Resolution resolution) {
        return new TimeInstant(getTime(), getTimezone(), resolution);
    }

    public TimeInstant next() {
        MutableDateTime time = new MutableDateTime(getTime());
        time.add(getResolution().getDateTimeFieldType().getDurationType(), 1);

        return new TimeInstant(time.getMillis(), getTimezone(), getResolution());
    }

    private TimeInstant(long time, DateTimeZone timezone, Resolution resolution) {
        this.time = time;
        this.timezone = timezone;
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
     * Get the timezone set for this TimeInstant
     *
     * @return the timezone
     */
    public DateTimeZone getTimezone() {
        return timezone;
    }

    /**
     * Get the {@link Resolution} set for this TimeInstant
     *
     * @return the resolution
     */
    public Resolution getResolution() {
        return resolution;
    }

    public boolean isAfter(TimeInstant timeInstant) {
        DateTime thisTime = new DateTime(getTime());
        DateTime thatTime = new DateTime(timeInstant.getTime());
        return thisTime.isAfter(thatTime);
    }

    public boolean compatibleWith(TimeInstant timeInstant) {
        return getResolution().equals(timeInstant.getResolution())
                && getTimezone().equals(timeInstant.getTimezone());
    }

    public static List<TimeInstant> getInstants(TimeInstant startTime, TimeInstant endTime) {
        if (startTime.isAfter(endTime)) {
            throw new IllegalArgumentException("startTime must be less than endTime");
        }

        if (!startTime.compatibleWith(endTime)) {
            throw new IllegalArgumentException("The timezone and resolution of startTime and endTime must match");
        }

        List<TimeInstant> result = new LinkedList<>();

        while (!startTime.isAfter(endTime)) {
            result.add(new TimeInstant(startTime.getTime(), startTime.getTimezone(), startTime.getResolution()));
            startTime = startTime.next();
        }

        return result;
    }

    public TimeInstantVO toValueObject() {
        return new TimeInstantVO(getTime(), getResolution().name(), getTimezone().getID());
    }

    public static TimeInstant fromValueObject(TimeInstantVO vo) {
        TimeInstant instant = TimeInstant.instant(vo.getTime());

        if (vo.getResolution() != null) {
            instant = instant.with(Resolution.valueOf(vo.getResolution().toUpperCase()));
        }

        if (vo.getTimezone() != null) {
            instant = instant.with(DateTimeZone.forTimeZone(TimeZone.getTimeZone(vo.getTimezone().toUpperCase())));
        }

        return instant;
    }
}
