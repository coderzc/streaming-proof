/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.lakestream.streaming.proof.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class DeliveryOrderTrackerTest {
    @Test
    public void kafkaChecksPartitionOffsetsAcrossDifferentKeys() {
        DeliveryOrderTracker t = new DeliveryOrderTracker();
        t.assigned(0);
        t.observe("a", 100, new MessageMetadata(0L, 0), DeliveryContext.kafka());
        t.observe("b", 0, new MessageMetadata(2L, 0), DeliveryContext.kafka());
        t.observe("c", 0, new MessageMetadata(1L, 0), DeliveryContext.kafka());
        assertFalse(t.violations().isEmpty());
    }

    @Test
    public void independentPartitionsAndConsumersDoNotCreateAnArtificialGlobalOrder() {
        DeliveryOrderTracker a = new DeliveryOrderTracker();
        DeliveryOrderTracker b = new DeliveryOrderTracker();
        a.assigned(0);
        b.assigned(1);
        a.observe("same", 20, new MessageMetadata(100L, 0), DeliveryContext.kafka());
        b.observe("same", 0, new MessageMetadata(0L, 1), DeliveryContext.kafka());
        assertTrue(a.violations().isEmpty());
        assertTrue(b.violations().isEmpty());
    }

    @Test
    public void revokedAndReassignedPartitionMayReplayFromCommittedOffset() {
        DeliveryOrderTracker t = new DeliveryOrderTracker();
        t.assigned(0);
        t.observe("key", 10, new MessageMetadata(10L, 0), DeliveryContext.kafka());
        t.revoked(0);
        t.assigned(0);
        t.observe("key", 5, new MessageMetadata(5L, 0), DeliveryContext.kafka());
        t.observe("key", 6, new MessageMetadata(6L, 0), DeliveryContext.kafka());
        assertTrue(t.violations().isEmpty());
    }

    @Test
    public void cooperativeAssignmentDoesNotResetRetainedPartitions() {
        DeliveryOrderTracker t = new DeliveryOrderTracker();
        t.assigned(0);
        t.observe("key", 10, new MessageMetadata(10L, 0), DeliveryContext.kafka());
        t.assigned(0);
        t.observe("key", 9, new MessageMetadata(9L, 0), DeliveryContext.kafka());
        assertFalse(t.violations().isEmpty());
    }

    @Test
    public void pulsarFailoverChecksAcrossKeysWhileKeySharedChecksOnlyWithinEachKey() {
        DeliveryOrderTracker failover = new DeliveryOrderTracker();
        failover.observe("a", 0, new MessageMetadata(1L, 5L), DeliveryContext.pulsarPartition(false));
        failover.observe("b", 0, new MessageMetadata(1L, 1L), DeliveryContext.pulsarPartition(false));
        assertFalse(failover.violations().isEmpty());
        DeliveryOrderTracker keyShared = new DeliveryOrderTracker();
        keyShared.observe("a", 0, new MessageMetadata(1L, 5L), DeliveryContext.pulsar(false));
        keyShared.observe("b", 0, new MessageMetadata(1L, 1L), DeliveryContext.pulsar(false));
        assertTrue(keyShared.violations().isEmpty());
    }

    @Test
    public void noAssignmentAndMissingMetadataAreCoverageGaps() {
        DeliveryOrderTracker t = new DeliveryOrderTracker();
        t.observe("key", 1, new MessageMetadata(1L, 0), DeliveryContext.kafka());
        t.observe("key", 2, null, null);
        assertEquals(t.gaps().size(), 2);
        assertEquals(t.observations(), 0L);
    }

    @Test
    public void pulsarDetectsFirstDeliveryAtAnEarlierLogPosition() {
        DeliveryOrderTracker t = new DeliveryOrderTracker();
        t.observe("key", 0, new MessageMetadata(1L, 0L), DeliveryContext.pulsar(false));
        t.observe("key", 2, new MessageMetadata(1L, 2L), DeliveryContext.pulsar(false));
        t.observe("key", 1, new MessageMetadata(1L, 1L), DeliveryContext.pulsar(false));
        assertFalse(t.violations().isEmpty());
    }

    @Test
    public void redeliveryAndAlreadySeenSequencesAreNotFirstDeliveryViolations() {
        DeliveryOrderTracker t = new DeliveryOrderTracker();
        t.observe("key", 2, new MessageMetadata(1L, 2L), DeliveryContext.pulsar(false));
        t.observe("key", 0, new MessageMetadata(1L, 0L), DeliveryContext.pulsar(true));
        t.observe("key", 2, new MessageMetadata(1L, 2L), DeliveryContext.pulsar(false));
        assertTrue(t.violations().isEmpty());
    }

    @Test
    public void producerBusinessSequenceIsNotConfusedWithDeliveryOrder() {
        DeliveryOrderTracker t = new DeliveryOrderTracker();
        t.observe("key", 2, new MessageMetadata(1L, 0L), DeliveryContext.pulsar(false));
        t.observe("key", 1, new MessageMetadata(1L, 1L), DeliveryContext.pulsar(false));
        assertTrue(t.violations().isEmpty());
    }

    @Test
    public void checkpointBackwardReadWithoutProvenanceIsInconclusiveNotFailed() {
        DeliveryOrderTracker t = new DeliveryOrderTracker();
        t.observe("key", 2, new MessageMetadata(1L, 2L), DeliveryContext.checkpoint());
        t.observe("key", 1, new MessageMetadata(1L, 1L), DeliveryContext.checkpoint());
        assertTrue(t.violations().isEmpty());
        assertFalse(t.gaps().isEmpty());
    }

    @Test
    public void pulsarPartitionsKeysAndBatchPositionsAreIndependent() {
        DeliveryOrderTracker t = new DeliveryOrderTracker();
        t.observe("a", 0, new MessageMetadata(null, 1L, 0L, 0, -1L, 1L, 1), DeliveryContext.pulsar(false));
        t.observe("a", 0, new MessageMetadata(null, 1L, 0L, 1, -1L, 1L, 0), DeliveryContext.pulsar(false));
        t.observe("b", 0, new MessageMetadata(null, 1L, 0L, 0, -1L, 1L, 0), DeliveryContext.pulsar(false));
        t.observe("a", 1, new MessageMetadata(null, 1L, 0L, 0, -1L, 1L, 2), DeliveryContext.pulsar(false));
        assertTrue(t.violations().isEmpty());
    }
}
