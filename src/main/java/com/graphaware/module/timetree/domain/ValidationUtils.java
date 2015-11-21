/*
 * Copyright (c) 2013-2015 GraphAware
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

package com.graphaware.module.timetree.domain;

/**
 * Utility class for input validation.
 */
public final class ValidationUtils {

    /**
     * Validate a range, i.e. that the start time is before end time and that the times are of the same resolution.
     *
     * @param startTime start.
     * @param endTime   end.
     * @throws IllegalArgumentException in case the range is invalid.
     */
    public static void validateRange(TimeInstant startTime, TimeInstant endTime) {
        if (startTime.isAfter(endTime)) {
            throw new IllegalArgumentException("startTime must be less than endTime");
        }

        if (!startTime.compatibleWith(endTime)) {
            throw new IllegalArgumentException("The timezone and resolution of startTime and endTime must match");
        }
    }

    private ValidationUtils() {
    }
}
