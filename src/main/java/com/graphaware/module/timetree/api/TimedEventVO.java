/*
 * Copyright (c) 2013-2019 GraphAware
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

package com.graphaware.module.timetree.api;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

/**
 * Wrapper of {@link EventVO} and {@link TimeInstantVO} for attaching events to time instants.
 */
public class TimedEventVO {

    @JsonUnwrapped
    private EventVO event;
    @JsonUnwrapped
    private TimeInstantVO timeInstant;

    public EventVO getEvent() {
        return event;
    }

    public void setEvent(EventVO event) {
        this.event = event;
    }

    public TimeInstantVO getTimeInstant() {
        return timeInstant;
    }

    public void setTimeInstant(TimeInstantVO timeInstant) {
        this.timeInstant = timeInstant;
    }

    public void validate() {
        event.validate();

        if (timeInstant == null) {
            throw new IllegalArgumentException("Time instant must not be null");
        }

        timeInstant.validate();
    }
}
