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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofConsumer;
import io.lakestream.streaming.proof.common.Util;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

public class StrictOrderingTaskTest {
    @Test
    public void firstDeliveryViolationSurvivesTrimmingSerializationAndAggregation() throws Exception {
        ProofConsumerTask task = new ProofConsumerTask(false, 8, true);
        task.setConsumer(mock(ProofConsumer.class));
        long position = 0;
        for (long seq : new long[] {0, 2, 1}) {
            task.onMessage("key", seq, new MessageMetadata(1L, position++));
        }
        task.applyHighWatermarks(Map.of("key", 2L));
        task.onMessage("key", 3L, new MessageMetadata(1L, position++));
        task.onMessage("key", 1L, new MessageMetadata(1L, 2L));
        assertEquals(task.getOrderingViolations().get("key"), List.of(Pair.of(1L, 1L)));
        ConsumerCheckPoint original = new ConsumerCheckPoint();
        original.addOrderingViolations(task.getOrderingViolations());
        ConsumerCheckPoint wire = Util.JSON_MAPPER.readValue(
                Util.JSON_MAPPER.writeValueAsBytes(original), ConsumerCheckPoint.class);
        ConsumerCheckPoint aggregated = new ConsumerCheckPoint();
        aggregated.merge(wire);
        aggregated.merge(wire);
        aggregated.calculate();
        assertEquals(aggregated.getOutOfOrderSeqs().get("key"), List.of(Pair.of(1L, 1L)));
    }

    @Test
    public void readerReplayAfterWatermarkDoesNotCreateAFirstDeliveryViolation() {
        ProofConsumerTask task = new ProofConsumerTask(false, 8, true);
        task.setConsumer(mock(ProofConsumer.class));
        for (long seq : new long[] {0, 1, 2}) {
            task.onMessage("key", seq, new MessageMetadata(1L, seq));
        }
        task.applyHighWatermarks(Map.of("key", 2L));
        for (long seq : new long[] {0, 1, 2, 3}) {
            task.onMessage("key", seq, new MessageMetadata(1L, seq));
        }
        assertTrue(task.getOrderingViolations().isEmpty());
    }

    @Test
    public void unorderedConsumersRejectStrictOrdering() {
        expectThrows(IllegalArgumentException.class, () -> new ProofConsumerTask(true, 8, true));
    }
}
