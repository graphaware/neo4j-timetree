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
