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
package io.lakestream.streaming.proof.worker;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.DeliveryContext;
import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofConsumer;
import io.lakestream.streaming.proof.common.Util;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import java.util.Map;
import org.testng.annotations.Test;

public class GeneralOrderingTaskTest {
    @Test
    public void violationSurvivesWatermarksTransportRepeatedMergeAndLaterCleanSnapshots() throws Exception {
        ProofConsumerTask task = task(false, false);
        task.onPartitionAssigned(0);
        task.onMessage("a", 0, new MessageMetadata(0L, 0), DeliveryContext.kafka());
        task.onMessage("b", 0, new MessageMetadata(2L, 0), DeliveryContext.kafka());
        task.onMessage("c", 0, new MessageMetadata(1L, 0), DeliveryContext.kafka());
        task.applyHighWatermarks(Map.of("a", 0L, "b", 0L, "c", 0L));
        ConsumerCheckPoint snapshot = new ConsumerCheckPoint();
        task.addOrderingEvidence(snapshot);
        ConsumerCheckPoint wire = Util.JSON_MAPPER.readValue(Util.JSON_MAPPER.writeValueAsBytes(snapshot),
                ConsumerCheckPoint.class);
        ConsumerCheckPoint merged = new ConsumerCheckPoint();
        merged.merge(wire);
        merged.merge(wire);
        merged.merge(new ConsumerCheckPoint());
        merged.calculate();
        assertFalse(merged.getOutOfOrderSeqs().isEmpty());
        assertEquals(merged.getOrderingEvidence().values().iterator().next().observations(), 3L);
    }

    @Test
    public void unorderedModeDoesNotAssertOrdering() {
        ProofConsumerTask task = task(true, false);
        task.onMessage("a", 2, new MessageMetadata(2L, 0), DeliveryContext.kafka());
        task.onMessage("a", 1, new MessageMetadata(1L, 0), DeliveryContext.kafka());
        ConsumerCheckPoint snapshot = new ConsumerCheckPoint();
        task.addOrderingEvidence(snapshot);
        assertTrue(snapshot.getOrderingEvidence().isEmpty());
        assertTrue(snapshot.getOutOfOrderSeqs().isEmpty());
    }

    @Test
    public void missingHandoffEvidenceRemainsExplicitAfterTransport() throws Exception {
        ProofConsumerTask task = task(false, true);
        task.onMessage("a", 0, new MessageMetadata(1L, 0L), DeliveryContext.pulsar(false));
        ConsumerCheckPoint checkpoint = new ConsumerCheckPoint();
        task.addOrderingEvidence(checkpoint);
        ConsumerCheckPoint wire = Util.JSON_MAPPER.readValue(Util.JSON_MAPPER.writeValueAsBytes(checkpoint),
                ConsumerCheckPoint.class);
        assertFalse(wire.getOrderingEvidence().values().iterator().next().gaps().isEmpty());
    }

    private static ProofConsumerTask task(boolean shared, boolean unobservedHandoffs) {
        ProofConsumerTask task = new ProofConsumerTask(shared, 8, true, unobservedHandoffs);
        task.setConsumer(mock(ProofConsumer.class));
        return task;
    }
}
