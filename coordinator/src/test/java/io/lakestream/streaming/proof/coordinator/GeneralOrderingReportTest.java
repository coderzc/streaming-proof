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

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.Util;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.ProofReport;
import io.lakestream.streaming.proof.common.records.PulsarProofConfig;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

public class GeneralOrderingReportTest {
    @Test
    public void missingDriverEvidenceCannotPassAnOrderingProof() {
        ProofTask task = task(null);
        try {
            assertEquals(task.getReport().status(), "inconclusive");
            assertEquals(task.getReport().orderingVerification().status(), "incomplete");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void violationFromAnUnfulfilledCheckpointStillFailsAndSurvivesReplacement() throws Exception {
        ProofTask task = task(null);
        try {
            ConsumerCheckPoint snapshot = new ConsumerCheckPoint();
            snapshot.addOrderingViolations(Map.of("partition:0", List.of(Pair.of(3L, 3L))));
            ConsumerCheckPoint wire = Util.JSON_MAPPER.readValue(Util.JSON_MAPPER.writeValueAsBytes(snapshot),
                    ConsumerCheckPoint.class);
            task.getLatestConsumerCheckpoint().merge(wire);
            assertEquals(task.getReport().status(), "failed");
            task.getLatestConsumerCheckpoint().setOrderingViolations(Map.of());
            assertEquals(task.getReport().summary().outOfOrders(), 1L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void multipleConsumerEvidenceIsMergedIdempotentlyAndReportedWithinScope() throws Exception {
        ProofTask task = task(null);
        try {
            for (String id : List.of("worker-a", "worker-b")) {
                task.getLatestConsumerCheckpoint().addOrderingEvidence(id,
                        new ConsumerCheckPoint.OrderingEvidence(10, Set.of()));
            }
            ProofReport report = task.getReport();
            assertEquals(report.status(), "passed");
            assertEquals(report.orderingVerification().observations(), 20L);
            assertEquals(report.orderingVerification().reportingConsumers(), 2);
            assertEquals(task.getReport().orderingVerification().observations(), 20L);
            ProofReport wire = Util.JSON_MAPPER.readValue(Util.JSON_MAPPER.writeValueAsBytes(report),
                    ProofReport.class);
            assertEquals(wire.orderingVerification().status(), "verified_within_scope");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void unsupportedHandoffIsInconclusiveAndSharedDoesNotAssertOrdering() {
        ProofTask task = task(null);
        try {
            task.getLatestConsumerCheckpoint().addOrderingEvidence("consumer",
                    new ConsumerCheckPoint.OrderingEvidence(10, Set.of("ownership not observed")));
            assertEquals(task.getReport().status(), "inconclusive");
            assertTrue(task.getReport().resultReason().contains("ownership not observed"));
        } finally {
            task.getExecutor().shutdownNow();
        }
        ProofTask shared = task(PulsarProofConfig.builder()
                .consumerConfig(Map.of("subscriptionType", "Shared")).build());
        try {
            assertEquals(shared.getReport().status(), "passed");
            assertEquals(shared.getReport().orderingVerification().status(), "not_applicable");
        } finally {
            shared.getExecutor().shutdownNow();
        }
    }

    @Test
    public void failedOrEmptyWorkerSnapshotsCannotReuseAnOldOrderingPass() throws Exception {
        ProofTask task = task(null);
        io.lakestream.streaming.proof.common.WorkerHttpClient client =
                mock(io.lakestream.streaming.proof.common.WorkerHttpClient.class);
        task.getClients().add(client);
        ConsumerCheckPoint complete = new ConsumerCheckPoint();
        for (String id : List.of("a", "b")) {
            complete.addOrderingEvidence(id, new ConsumerCheckPoint.OrderingEvidence(10, Set.of()));
        }
        task.getLatestConsumerCheckpoint().merge(complete);
        org.mockito.Mockito.when(client.producerCheckpoint(org.mockito.ArgumentMatchers.any()))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(
                        new io.lakestream.streaming.proof.common.records.ProducerCheckpoint()));
        org.mockito.Mockito.when(client.consumerCheckpoint(org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any())).thenReturn(
                        java.util.concurrent.CompletableFuture.failedFuture(new RuntimeException("worker unavailable")),
                        java.util.concurrent.CompletableFuture.completedFuture(new ConsumerCheckPoint()),
                        java.util.concurrent.CompletableFuture.completedFuture(complete));
        try {
            assertEquals(task.getReport().status(), "passed");
            org.testng.Assert.expectThrows(RuntimeException.class, task::aggregateCheckpoints);
            assertEquals(task.getReport().status(), "inconclusive");
            task.aggregateCheckpoints();
            assertEquals(task.getReport().status(), "inconclusive");
            task.aggregateCheckpoints();
            assertEquals(task.getReport().status(), "passed");
            task.getLatestConsumerCheckpoint().addOrderingEvidence("replacement",
                    new ConsumerCheckPoint.OrderingEvidence(1, Set.of()));
            assertEquals(task.getReport().status(), "inconclusive");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void sharedDriverDefaultsDisableOrderingAndPerProofOverrideRestoresIt() {
        var driver = new io.lakestream.streaming.proof.common.records.Driver("pulsar",
                Map.of("pulsar.consumer.config", Map.of("subscriptionType", "Shared")));
        Configs configs = new Configs(Map.of(), Map.of("driver", driver));
        for (boolean override : List.of(false, true)) {
            Proof proof = Proof.builder().driver("driver").consumers(2)
                    .features(List.of("at_least_once", "ordering"))
                    .pulsar(override ? PulsarProofConfig.builder()
                            .consumerConfig(Map.of("subscriptionType", "Key_Shared")).build() : null).build();
            ProofTask task = new ProofTask(proof, configs, mock(ProofDriver.class));
            try {
                assertEquals(task.getReport().status(), override ? "inconclusive" : "passed");
            } finally {
                task.getExecutor().shutdownNow();
            }
        }
    }

    private static ProofTask task(PulsarProofConfig pulsar) {
        Proof proof = Proof.builder().driver("driver").topic("test").consumers(2)
                .features(List.of("at_least_once", "ordering")).pulsar(pulsar).build();
        return new ProofTask(proof, new Configs(Map.of(), Map.of()), mock(ProofDriver.class));
    }
}
