package com.graphaware.module.timetree.api;

import com.graphaware.module.timetree.SingleTimeTree;
import com.graphaware.module.timetree.TimeTree;
import com.graphaware.module.timetree.TimeTreeBackedEvents;
import com.graphaware.module.timetree.TimedEvents;
import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration of beans wired into the APIs.
 */
@Configuration
public class TimeTreeSpringConfig {

    @Autowired
    private GraphDatabaseService database;

    @Bean
    public TimeTree timeTree() {
        return new SingleTimeTree(database);
    }

    @Bean
    public TimedEvents timedEvents() {
        return new TimeTreeBackedEvents(timeTree());
    }
}
