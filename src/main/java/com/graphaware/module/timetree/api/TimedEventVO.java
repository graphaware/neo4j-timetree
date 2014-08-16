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
