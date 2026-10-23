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
package io.lakestream.streaming.proof.coordinator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.LongSeq;
import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.WorkerHttpClient;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.ProducerCheckpoint;
import io.lakestream.streaming.proof.common.records.Proof;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class OrderingRangeSummaryTest {
    @Test
    public void deliveryOnlyProofsDoNotAssertOrdering() {
        Proof proof = Proof.builder().features(List.of("at_least_once")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), mock(ProofDriver.class));
        try {
            task.getLastVerifiedConsumerCheckpoint().mergeOutOfOrderSeqs(
                    Map.of("key", List.of(org.apache.commons.lang3.tuple.Pair.of(1L, 1L))));
            assertEquals(task.getSummary().outOfOrders(), 0);
            assertTrue(task.getDetails().outOfOrderSeqs().isEmpty());
            assertEquals(task.getReport().status(), "passed");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void requestWatermarkExcludesVerifiedReplayFromPermanentEvidence() throws Exception {
        Proof proof = Proof.builder().id("proof").features(List.of("at_least_once", "ordering")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), mock(ProofDriver.class));
        WorkerHttpClient client = mock(WorkerHttpClient.class);
        task.getClients().add(client);
        Map<String, Long> watermarks = task.getHighWatermarks();
        watermarks.put("key", 1L);
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        var clipped = new ConsumerCheckPoint.SeqRange();
        clipped.setStart(new LongSeq(2L, new MessageMetadata(0L)));
        clipped.setEnd(new LongSeq(2L, new MessageMetadata(2L)));
        var replay = new ConsumerCheckPoint.SeqRange();
        replay.setStart(new LongSeq(1L, new MessageMetadata(1L)));
        replay.setEnd(replay.getStart());
        cp.addKey("key", Map.of("clipped", clipped, "replay", replay));
        when(client.producerCheckpoint(any())).thenReturn(CompletableFuture.completedFuture(new ProducerCheckpoint()));
        when(client.consumerCheckpoint(any(), any())).thenReturn(CompletableFuture.completedFuture(cp),
                CompletableFuture.completedFuture(new ConsumerCheckPoint()));
        try {
            ConsumerCheckPoint snapshot = task.aggregateCheckpoints().getRight();
            assertTrue(snapshot.getOutOfOrderSeqs().isEmpty());
            var verified = ProofTask.class.getDeclaredField("lastVerifiedConsumerCheckpoint");
            verified.setAccessible(true);
            verified.set(task, snapshot);
            watermarks.clear(); // A later coordinator update must not change the snapshot's base.
            assertEquals(task.getSummary().outOfOrders(), 0);
            assertTrue(task.getDetails().outOfOrderSeqs().isEmpty());
            task.setLatestConsumerCheckpoint(task.aggregateCheckpoints().getRight());
            assertEquals(task.getReport().status(), "passed");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void unfulfilledAndLaterEmptySnapshotsCannotEraseAConfirmedViolation() throws Exception {
        Proof proof = Proof.builder().id("proof").features(List.of("at_least_once", "ordering")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), mock(ProofDriver.class));
        WorkerHttpClient client = mock(WorkerHttpClient.class);
        task.getClients().add(client);
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        for (int i = 0; i < 3; i++) {
            var range = new ConsumerCheckPoint.SeqRange();
            range.setStart(new LongSeq(new long[] {0, 2, 1}[i], new MessageMetadata((long) i)));
            range.setEnd(range.getStart());
            cp.addKey("key", Map.of("range" + i, range));
        }
        when(client.producerCheckpoint(any())).thenReturn(CompletableFuture.completedFuture(new ProducerCheckpoint()));
        when(client.consumerCheckpoint(any(), any())).thenReturn(CompletableFuture.completedFuture(cp),
                CompletableFuture.completedFuture(new ConsumerCheckPoint()));
        try {
            task.setLatestConsumerCheckpoint(task.aggregateCheckpoints().getRight());
            assertEquals(task.getSummary().outOfOrders(), 1);
            assertEquals(task.getReport().status(), "failed");
            task.setLatestConsumerCheckpoint(task.aggregateCheckpoints().getRight());
            assertEquals(task.getSummary().outOfOrders(), 1);
            assertEquals(task.getReport().status(), "failed");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }
}
