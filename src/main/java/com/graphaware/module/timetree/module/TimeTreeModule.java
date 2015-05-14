/*
 * Copyright (c) 2015 GraphAware
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

package com.graphaware.module.timetree.module;

import com.graphaware.module.timetree.CustomRootTimeTree;
import com.graphaware.module.timetree.SingleTimeTree;
import com.graphaware.module.timetree.TimeTreeBackedEvents;
import com.graphaware.module.timetree.TimedEvents;
import com.graphaware.module.timetree.domain.TimeInstant;
import com.graphaware.runtime.config.TxDrivenModuleConfiguration;
import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
import com.graphaware.tx.event.improved.api.Change;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.graphaware.tx.executor.batch.IterableInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import com.graphaware.tx.executor.single.TransactionCallback;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link com.graphaware.runtime.module.TxDrivenModule} that automatically attaches events to a {@link com.graphaware.module.timetree.TimeTree}.
 */
public class TimeTreeModule extends BaseTxDrivenModule<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(TimeTreeModule.class);

    private final TimeTreeConfiguration configuration;
    private final TimedEvents timedEvents;
    private GraphDatabaseService database;

    public TimeTreeModule(String moduleId, TimeTreeConfiguration configuration, GraphDatabaseService database) {
        super(moduleId);
        this.configuration = configuration;
        this.timedEvents = new TimeTreeBackedEvents(new SingleTimeTree(database));
        this.database = database;
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
            if (transactionData.hasPropertyBeenChanged(change.getPrevious(), configuration.getTimestampProperty())) {
                deleteTimeTreeRelationship(change.getPrevious());
                createTimeTreeRelationship(change.getCurrent());
            }
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(GraphDatabaseService database) {
        if (!configuration.isAutoAttach()) {
            return;
        }

        new IterableInputBatchTransactionExecutor<>(database, 1000, new TransactionCallback<Iterable<Node>>() {
            @Override
            public Iterable<Node> doInTransaction(GraphDatabaseService database) throws Exception {
                return GlobalGraphOperations.at(database).getAllNodes();
            }
        }, new UnitOfWork<Node>() {
            @Override
            public void execute(GraphDatabaseService database, Node input, int batchNumber, int stepNumber) {
                if (stepNumber == 1) {
                    LOG.info("Attaching existing events to TimeTree in batch " + batchNumber);
                }
                if (configuration.getInclusionPolicies().getNodeInclusionPolicy().include(input)) {
                    createTimeTreeRelationship(input);
                }
            }
        }).execute();
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


        if (created.hasProperty(configuration.getCustomTimeTreeRootProperty())){
            Long rootId = Long.valueOf(created.getProperty(configuration.getCustomTimeTreeRootProperty()).toString());
            Node root = database.getNodeById(rootId);
            TimeTreeBackedEvents customRootTimeTree = new TimeTreeBackedEvents(new CustomRootTimeTree(root));
            customRootTimeTree.attachEvent(created, configuration.getRelationshipType(), TimeInstant.instant(timestamp).with(configuration.getResolution()).with(configuration.getTimeZone()));

            return;
        }

        timedEvents.attachEvent(created, configuration.getRelationshipType(), TimeInstant.instant(timestamp).with(configuration.getResolution()).with(configuration.getTimeZone()));
    }

    private void deleteTimeTreeRelationship(Node changed) {
        for (Relationship r : changed.getRelationships(Direction.OUTGOING, configuration.getRelationshipType())) {
            r.delete();
        }
    }
}
