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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.WorkerHttpClient;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.Driver;
import io.lakestream.streaming.proof.common.records.NewProducers;
import io.lakestream.streaming.proof.common.records.Proof;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ProducerDistributionTest {
    @DataProvider
    public Object[][] allocations() {
        return new Object[][] {
                {1, 16, 100, 1},
                {2, 16, 100, 2},
                {3, 16, 100, 3},
                {5, 16, 100, 3},
                {3, 1, 100, 1},
                {3, 2, 100, 2},
                {3, 16, 1, 1},
                {3, 16, 2, 2}
        };
    }

    @Test(dataProvider = "allocations")
    public void distributeOnlyNonEmptyProducerGroups(int producers, int keys, int rate, int activeWorkers)
            throws Exception {
        Proof proof = Proof.builder().id("allocation").topic("allocation")
                .driver("pulsar").features(List.of("at_least_once"))
                .producers(producers).keys(keys).msgRate(rate).build();
        ProofTask task = new ProofTask(proof,
                new Configs(Map.of(), Map.of("pulsar", new Driver("pulsar", Map.of()))),
                mock(ProofDriver.class));
        List<NewProducers> assignments = new ArrayList<>();
        List<WorkerHttpClient> workers = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                WorkerHttpClient worker = mock(WorkerHttpClient.class);
                when(worker.startProducers(any())).thenAnswer(invocation -> {
                    assignments.add(invocation.getArgument(0));
                    return CompletableFuture.completedFuture(null);
                });
                task.getClients().add(worker);
                workers.add(worker);
            }
            Method start = ProofTask.class.getDeclaredMethod("startProducers");
            start.setAccessible(true);
            start.invoke(task);

            assertEquals(assignments.size(), activeWorkers);
            assertEquals(assignments.stream().mapToInt(NewProducers::producers).sum(), producers);
            assertEquals(assignments.stream().mapToInt(NewProducers::keys).sum(), keys);
            assertEquals(assignments.stream().mapToInt(NewProducers::msgRate).sum(), rate);
            for (NewProducers assignment : assignments) {
                assertTrue(assignment.producers() > 0);
                assertTrue(assignment.keys() > 0);
                assertTrue(assignment.msgRate() > 0);
                assertEquals(assignment.id(), proof.getId());
                assertEquals(assignment.driverName(), "pulsar");
            }
            for (int i = activeWorkers; i < workers.size(); i++) {
                verify(workers.get(i), never()).startProducers(any());
            }
        } finally {
            task.getExecutor().shutdownNow();
        }
    }
}
