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

import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import static com.graphaware.module.timetree.domain.Resolution.DAY;
import static com.graphaware.module.timetree.domain.ValidationUtils.validateRange;

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

    /**
     * Create an instant immediately following the current one, i.e. with its resolution unit incremented by 1.
     *
     * @return next instant.
     */
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
     * Get the UTC time in ms from 1/1/1970 for this instant.
     *
     * @return the UTC time in ms from 1/1/1970.
     */
    public long getTime() {
        return time;
    }

    /**
     * Get the timezone set for this instant.
     *
     * @return the timezone.
     */
    public DateTimeZone getTimezone() {
        return timezone;
    }

    /**
     * Get the {@link Resolution} set for this instant.
     *
     * @return the resolution.
     */
    public Resolution getResolution() {
        return resolution;
    }

    /**
     * Check if this instant is after another one.
     *
     * @param timeInstant to check for.
     * @return true iff this instant is after the given one.
     */
    public boolean isAfter(TimeInstant timeInstant) {
        DateTime thisTime = new DateTime(getTime());
        DateTime thatTime = new DateTime(timeInstant.getTime());
        return thisTime.isAfter(thatTime);
    }

    /**
     * Check if this instant is compatible with the given instant, i.e., that their resolutions are the same.
     *
     * @param timeInstant to check.
     * @return true iff compatible.
     */
    public boolean compatibleWith(TimeInstant timeInstant) {
        return getResolution().equals(timeInstant.getResolution())
                && getTimezone().equals(timeInstant.getTimezone());
    }

    /**
     * Get instants between two instants (inclusive). Both instants provided to this method must have the same resolution
     * and start instant must not have happened after end instant.
     *
     * @param startTime start.
     * @param endTime   end.
     * @return all instants in between with the same resolution as the start and end instant.
     */
    public static List<TimeInstant> getInstants(TimeInstant startTime, TimeInstant endTime) {
        validateRange(startTime, endTime);

        List<TimeInstant> result = new LinkedList<>();

        while (!startTime.isAfter(endTime)) {
            result.add(new TimeInstant(startTime.getTime(), startTime.getTimezone(), startTime.getResolution()));
            startTime = startTime.next();
        }

        return result;
    }

    /**
     * Create an instant from its corresponding value object.
     *
     * @param vo to create the instant from.
     * @return instant.
     */
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
