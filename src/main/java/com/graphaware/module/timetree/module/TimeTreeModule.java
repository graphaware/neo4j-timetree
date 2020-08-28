/*
 * Copyright (c) 2013-2020 GraphAware
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

package com.graphaware.module.timetree.module;

import com.graphaware.common.log.LoggerFactory;
import com.graphaware.common.util.Change;
import com.graphaware.module.timetree.CustomRootTimeTree;
import com.graphaware.module.timetree.SingleTimeTree;
import com.graphaware.module.timetree.TimeTreeBackedEvents;
import com.graphaware.module.timetree.TimedEvents;
import com.graphaware.module.timetree.domain.TimeInstant;
import com.graphaware.runtime.config.TxDrivenModuleConfiguration;
import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.graphaware.tx.executor.batch.IterableInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import com.graphaware.tx.executor.input.TransactionalInput;
import com.graphaware.tx.executor.single.TransactionCallback;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;

import static com.graphaware.common.util.EntityUtils.getLong;

/**
 * A {@link com.graphaware.runtime.module.TxDrivenModule} that automatically attaches events to a {@link com.graphaware.module.timetree.TimeTree}.
 */
public class TimeTreeModule extends BaseTxDrivenModule<Void> {

    private static final Log LOG = LoggerFactory.getLogger(TimeTreeModule.class);

    private final TimeTreeConfiguration configuration;
    private final TimedEvents timedEvents;

    public TimeTreeModule(String moduleId, TimeTreeConfiguration configuration, GraphDatabaseService database) {
        super(moduleId);
        this.configuration = configuration;
        this.timedEvents = new TimeTreeBackedEvents(new SingleTimeTree(database));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TxDrivenModuleConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Void beforeCommit(ImprovedTransactionData transactionData) throws DeliberateTransactionRollbackException {
        for (Node created : transactionData.getAllCreatedNodes()) {
            createTimeTreeRelationship(created);
        }

        for (Change<Node> change : transactionData.getAllChangedNodes()) {
            if (shouldReattach(transactionData, change)) {
                deleteTimeTreeRelationship(change.getPrevious());
                createTimeTreeRelationship(change.getCurrent());
            }
        }

        return null;
    }

    private boolean shouldReattach(ImprovedTransactionData transactionData, Change<Node> change) {
        return transactionData.hasPropertyBeenCreated(change.getCurrent(), configuration.getTimestampProperty())
                || transactionData.hasPropertyBeenCreated(change.getCurrent(), configuration.getCustomTimeTreeRootProperty())
                || transactionData.hasPropertyBeenChanged(change.getPrevious(), configuration.getTimestampProperty())
                || transactionData.hasPropertyBeenChanged(change.getPrevious(), configuration.getCustomTimeTreeRootProperty())
                || transactionData.hasPropertyBeenDeleted(change.getPrevious(), configuration.getTimestampProperty())
                || transactionData.hasPropertyBeenDeleted(change.getPrevious(), configuration.getCustomTimeTreeRootProperty());
    }

    private void createTimeTreeRelationship(Node created) {
        if (!created.hasProperty(configuration.getTimestampProperty())) {
            LOG.warn("Created node with ID " + created.getId() + " does not have a " + configuration.getTimestampProperty() + " property!");
            return;
        }

        Long timestamp;
        try {
            timestamp = (Long) created.getProperty(configuration.getTimestampProperty());
        } catch (Throwable throwable) {
            LOG.warn("Created node with ID " + created.getId() + " does not have a valid timestamp property", throwable);
            return;
        }

        TimedEvents timedEventsToUse;
        if (configuration.getCustomTimeTreeRootProperty() != null && created.hasProperty(configuration.getCustomTimeTreeRootProperty())) {
            timedEventsToUse = new TimeTreeBackedEvents(new CustomRootTimeTree(created.getGraphDatabase().getNodeById(getLong(created, configuration.getCustomTimeTreeRootProperty()))));
        } else {
            timedEventsToUse = timedEvents;
        }

        timedEventsToUse.attachEvent(created, configuration.getRelationshipType(), configuration.getDirection(), TimeInstant.instant(timestamp).with(configuration.getResolution()).with(configuration.getTimeZone()));
    }

    private void deleteTimeTreeRelationship(Node changed) {
        for (Relationship r : changed.getRelationships(Direction.OUTGOING, configuration.getRelationshipType())) {
            r.delete();
        }
    }
}
