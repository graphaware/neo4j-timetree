package com.graphaware.module.timetree.api;

/**
 * Created by luanne on 28/07/14.
 */
public class EventTime {

    private long time;
    private String resolution;
    private String timezone;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }
}
