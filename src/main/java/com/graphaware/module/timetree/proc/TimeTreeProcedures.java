/*
 * Copyright (c) 2013-2016 GraphAware
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

package com.graphaware.module.timetree.proc;

import com.graphaware.module.timetree.TimedEvents;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.neo4j.kernel.api.exceptions.KernelException;

@Component
public class TimeTreeProcedures {

    private final GraphDatabaseService database;
    private final Procedures procedures;
    private final TimedEvents timedEvents;


    @Autowired
    public TimeTreeProcedures(GraphDatabaseService database, TimedEvents timedEvents, Procedures procedures) {
        this.database = database;
        this.procedures = procedures;
        this.timedEvents = timedEvents;
    }

    @PostConstruct
    public void init() throws KernelException {
        TimeTreeProcedure timeTreeProcedures = new TimeTreeProcedure(database);
        procedures.register(timeTreeProcedures.get());
        procedures.register(timeTreeProcedures.now());
        procedures.register(timeTreeProcedures.getInstants());
        TimedEventsProcedure timedEventsProcedures = new TimedEventsProcedure(database, timedEvents);
        procedures.register(timedEventsProcedures.getEvents());
        procedures.register(timedEventsProcedures.getRangeEvents());
        procedures.register(timedEventsProcedures.getAttach());
    }
}
