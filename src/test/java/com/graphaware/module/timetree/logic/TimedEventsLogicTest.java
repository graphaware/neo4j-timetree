package com.graphaware.module.timetree.logic;

import com.graphaware.module.timetree.SingleTimeTree;
import com.graphaware.module.timetree.TimeTreeBackedEvents;
import com.graphaware.module.timetree.TimedEvents;
import com.graphaware.test.integration.GraphAwareIntegrationTest;
import org.junit.Test;
import org.neo4j.graphdb.*;

import static org.junit.Assert.*;

public class TimedEventsLogicTest extends GraphAwareIntegrationTest {

    private static final String CUSTOM_ROOT_LABEL = "CustomRoot";

    @Test
    public void testAttachEventWithCustomRoot() {
        long rootId = createRoot();
        TimedEvents timedEvents = new TimeTreeBackedEvents(new SingleTimeTree(getDatabase()));
        TimedEventsBusinessLogic logic = new TimedEventsBusinessLogic(getDatabase(), timedEvents);
        boolean attached = logic.attachEventWithCustomRoot(getNodeById(rootId), createEvent(),
                RelationshipType.withName("SENT_ON"), Direction.OUTGOING.toString(), System.currentTimeMillis(), null, null);
        assertEquals(true, attached);
        try (Transaction tx = getDatabase().beginTx()) {
            Node root = getDatabase().getNodeById(rootId);
            Node year = root.getSingleRelationship(RelationshipType.withName("CHILD"), Direction.OUTGOING).getEndNode();
            assertTrue(year.hasLabel(Label.label("Year")));
            tx.success();
        }
    }

    private Node createEvent() {
        Node node;
        try (Transaction tx = getDatabase().beginTx()) {
            node = getDatabase().createNode();
            tx.success();
        }

        return node;
    }

    private Node getNodeById(long id) {
        Node node;
        try (Transaction tx = getDatabase().beginTx()) {
            node = getDatabase().getNodeById(id);
            tx.success();
        }

        return node;
    }

    private long createRoot() {
        long id;
        try (Transaction tx = getDatabase().beginTx()) {
            Node node = getDatabase().createNode(Label.label(CUSTOM_ROOT_LABEL));
            id = node.getId();
            tx.success();
        }

        return id;
    }

}
