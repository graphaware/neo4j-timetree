package com.graphaware.module.timetree.api;

/**
 * Created by luanne on 28/07/14.
 */
public class Event {

    private EventTime eventTime;
    private long eventNodeId;
    private String eventRelationType;
    private String eventRelationDirection;

    public long getEventNodeId() {
        return eventNodeId;
    }

    public void setEventNodeId(long eventNodeId) {
        this.eventNodeId = eventNodeId;
    }

    public EventTime getEventTime() {
        return eventTime;
    }

    public void setEventTime(EventTime eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventRelationType() {
        return eventRelationType;
    }

    public void setEventRelationType(String eventRelationType) {
        this.eventRelationType = eventRelationType;
    }

    public String getEventRelationDirection() {
        return eventRelationDirection;
    }

    public void setEventRelationDirection(String eventRelationDirection) {
        this.eventRelationDirection = eventRelationDirection;
    }
}
