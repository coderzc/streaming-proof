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

import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofConsumer;
import io.lakestream.streaming.proof.common.Util;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

public class OrderingRangeCheckpointTest {
    @Test
    public void preservesLateFirstDeliveriesThroughTheExistingWorkerCompactionAndWireFormat() throws Exception {
        ProofConsumerTask task = new ProofConsumerTask();
        task.setConsumer(mock(ProofConsumer.class));
        long[] sequences = {0, 3, 1, 2, 3, 4};
        for (int i = 0; i < sequences.length; i++) {
            task.onMessage("key", sequences[i], new MessageMetadata((long) i));
        }
        ConsumerCheckPoint cp = checkpoint(task);
        assertEquals(cp.getOutOfOrderSeqs().get("key"), List.of(Pair.of(1L, 2L)));
        assertTrue(cp.getMissedSeqs().isEmpty());
        task.applyHighWatermarks(Map.of("key", 4L));
        cp.mergeOutOfOrderSeqs(checkpoint(task).getOutOfOrderSeqs());
        cp.calculate();
        assertEquals(cp.getOutOfOrderSeqs().get("key"), List.of(Pair.of(1L, 2L)));
    }

    @Test
    public void reorderedReceptionOfEarlierLogPositionsIsNotConfusedWithANewLateWrite() throws Exception {
        ProofConsumerTask task = new ProofConsumerTask();
        task.setConsumer(mock(ProofConsumer.class));
        task.onMessage("key", 0, new MessageMetadata(0L));
        task.onMessage("key", 2, new MessageMetadata(2L));
        task.onMessage("key", 1, new MessageMetadata(1L));
        assertTrue(checkpoint(task).getOutOfOrderSeqs().isEmpty());
    }

    @Test
    public void duplicateCompactionRetainsTheEarliestPositionForEqualSequenceBoundaries() throws Exception {
        ProofConsumerTask task = new ProofConsumerTask();
        task.setConsumer(mock(ProofConsumer.class));
        long[] sequences = {0, 2, 0, 1};
        long[] offsets = {0, 2, 3, 1};
        for (int i = 0; i < sequences.length; i++) {
            task.onMessage("key", sequences[i], new MessageMetadata(offsets[i]));
        }
        assertTrue(checkpoint(task).getOutOfOrderSeqs().isEmpty());
    }

    @Test
    public void driversWithoutComparablePositionsKeepConsecutiveMessagesInOneRange() throws Exception {
        ProofConsumerTask task = new ProofConsumerTask();
        task.setConsumer(mock(ProofConsumer.class));
        task.onMessage("key", 0, MessageMetadata.empty());
        task.onMessage("key", 1, MessageMetadata.empty());
        assertEquals(task.getConsumed().get("key").size(), 1);
        assertTrue(checkpoint(task).getOutOfOrderSeqs().isEmpty());
    }

    private static ConsumerCheckPoint checkpoint(ProofConsumerTask task) throws Exception {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        task.getTrimmedConsumed().forEach(cp::addKey);
        task.getWriteDupsOrOutOrder().forEach(cp::addWriteDupsOrOutOrder);
        cp = Util.JSON_MAPPER.readValue(Util.JSON_MAPPER.writeValueAsBytes(cp), ConsumerCheckPoint.class);
        cp.calculate();
        return cp;
    }
}
