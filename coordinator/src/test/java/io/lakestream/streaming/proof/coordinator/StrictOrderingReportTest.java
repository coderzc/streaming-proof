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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.FirstDeliveryOrderTracker;
import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.Util;
import io.lakestream.streaming.proof.common.WorkerHttpClient;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.Driver;
import io.lakestream.streaming.proof.common.records.NewConsumers;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.PulsarProofConfig;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class StrictOrderingReportTest {
    @Test
    public void transportedCumulativeViolationFailsFinalReportAndDoesNotDisappear() throws Exception {
        ProofTask task = task(true);
        try {
            FirstDeliveryOrderTracker tracker = new FirstDeliveryOrderTracker();
            tracker.observe("key", 0);
            tracker.observe("key", 2);
            tracker.observe("key", 1);
            ConsumerCheckPoint snapshot = new ConsumerCheckPoint();
            snapshot.addOrderingViolations(tracker.violations());
            ConsumerCheckPoint transported = Util.JSON_MAPPER.readValue(
                    Util.JSON_MAPPER.writeValueAsBytes(snapshot), ConsumerCheckPoint.class);
            task.getLastVerifiedConsumerCheckpoint().merge(transported);
            assertEquals(task.getReport().status(), "failed");
            assertEquals(task.getReport().summary().outOfOrders(), 1L);
            task.getLastVerifiedConsumerCheckpoint().merge(transported);
            task.getLastVerifiedConsumerCheckpoint().calculate();
            assertEquals(task.getReport().summary().outOfOrders(), 1L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void legacyScalableReportQualifiesItsOrderingClaim() {
        ProofTask task = task(false);
        try {
            assertTrue(task.getReport().resultReason().contains("not independently validated"));
        } finally {
            task.getExecutor().shutdownNow();
        }
        ProofTask strict = task(true);
        try {
            assertFalse(strict.getReport().resultReason().contains("not independently validated"));
        } finally {
            strict.getExecutor().shutdownNow();
        }
    }

    @Test
    public void workerRequestPreservesStrictFlagAndOlderRequestDefaultsItOff() throws Exception {
        NewConsumers request = new NewConsumers("proof", "topic://public/default/test", 2, 1, 0, 8,
                "pulsar", new Driver("pulsar", Map.of(), null), null, "stream", true);
        NewConsumers decoded = Util.JSON_MAPPER.readValue(
                Util.JSON_MAPPER.writeValueAsBytes(request), NewConsumers.class);
        assertTrue(decoded.strictOrdering());
        String oldRequest = Util.JSON_MAPPER.writeValueAsString(request)
                .replace(",\"strictOrdering\":true", "");
        assertFalse(Util.JSON_MAPPER.readValue(oldRequest, NewConsumers.class).strictOrdering());
    }

    @Test
    public void coordinatorPassesStrictModeToTheWorkerRequest() throws Exception {
        ProofTask task = task(true);
        WorkerHttpClient worker = mock(WorkerHttpClient.class);
        when(worker.startConsumers(any())).thenReturn(CompletableFuture.completedFuture(null));
        task.getClients().add(worker);
        try {
            task.startConsumers();
            ArgumentCaptor<NewConsumers> request = ArgumentCaptor.forClass(NewConsumers.class);
            verify(worker).startConsumers(request.capture());
            assertTrue(request.getValue().strictOrdering());
            assertEquals(request.getValue().consumers(), 1);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    private static ProofTask task(boolean strict) {
        Proof proof = Proof.builder().driver("pulsar").topic("topic://public/default/strict-test")
                .consumers(1).features(List.of("at_least_once", "ordering"))
                .pulsar(PulsarProofConfig.builder().scalableTopicEnabled(true).consumerType("stream")
                        .strictOrdering(strict).build()).build();
        return new ProofTask(proof, new Configs(Map.of(), Map.of()), mock(ProofDriver.class));
    }
}
