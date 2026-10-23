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
package io.lakestream.streaming.proof.common.records;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.CoordinatorHttpClient;
import io.lakestream.streaming.proof.common.LongSeq;
import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.Util;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;
import org.testng.annotations.Test;

public class ConsumerOrderingRangesTest {
    @Test
    public void fillsTheOrderingResultWithoutCallingACompletedSequenceGapMissing() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        add(cp, "a", 0, 0, 0, 0);
        add(cp, "b", 2, 2, 1, 1);
        add(cp, "c", 1, 1, 2, 2);
        cp.calculate();
        assertEquals(cp.getOutOfOrderSeqs().get("key"), List.of(Pair.of(1L, 1L)));
        assertTrue(cp.getMissedSeqs().isEmpty());
        cp.calculate();
        assertEquals(cp.getOutOfOrderSeqs().get("key"), List.of(Pair.of(1L, 1L)));
    }

    @Test
    public void detectsAnEntireLateRangeFromTheExistingTimestampRangesRegression() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        add(cp, "a", 0, 4607, 19, 79922);
        add(cp, "b", 4635, 4635, 80449, 80449);
        add(cp, "c", 4608, 4634, 80686, 80738);
        cp.calculate();
        assertEquals(cp.getOutOfOrderSeqs().get("key"), List.of(Pair.of(4608L, 4634L)));
        assertTrue(cp.getMissedSeqs().isEmpty());
    }

    @Test
    public void ignoresRepeatedWritesAndReplaysOfSequencesAlreadyPresent() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        add(cp, "a", 0, 5, 0, 5);
        add(cp, "b", 2, 3, 2, 3);
        add(cp, "c", 2, 3, 6, 7);
        cp.calculate();
        assertTrue(cp.getOutOfOrderSeqs().isEmpty());
    }

    @Test
    public void messagePositionsRatherThanWorkerTimestampKeysEstablishLogOrder() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        add(cp, "worker-clock-early", 2, 2, 2, 2);
        add(cp, "worker-clock-late", 0, 1, 0, 1);
        cp.calculate();
        assertTrue(cp.getOutOfOrderSeqs().isEmpty());
    }

    @Test
    public void doesNotCompareIndependentPartitionsOrMissingPositions() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        ConsumerCheckPoint.SeqRange a = range(10, 10, 0, 0);
        ConsumerCheckPoint.SeqRange b = range(0, 0, 1, 1);
        a.setStart(new LongSeq(10, new MessageMetadata(0L, 0)));
        a.setEnd(a.getStart());
        b.setStart(new LongSeq(0, new MessageMetadata(1L, 1)));
        b.setEnd(b.getStart());
        cp.addKey("key", Map.of("a", a, "b", b));
        add(cp, "c", 0, 0, -1, -1);
        cp.calculate();
        assertTrue(cp.getOutOfOrderSeqs().isEmpty());
    }

    @Test
    public void overlappingCompactionMustNotEraseAnOrderingInversion() {
        ConsumerCheckPoint.SeqRange earlier = range(3, 5, 0, 2);
        ConsumerCheckPoint.SeqRange later = range(1, 6, 3, 8);
        assertFalse(earlier.merge(later));
        assertEquals(earlier.getStart().seq(), 3L);
        assertEquals(later.getStart().seq(), 1L);
    }

    @Test
    public void workerWireRangesAreEnoughAndConfirmedEvidenceSurvivesTrimming() throws Exception {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        add(cp, "a", 2, 2, 0, 0);
        add(cp, "b", 0, 1, 1, 2);
        ConsumerCheckPoint wire = Util.JSON_MAPPER.readValue(Util.JSON_MAPPER.writeValueAsBytes(cp),
                ConsumerCheckPoint.class);
        wire.calculate();
        assertEquals(wire.getOutOfOrderSeqs().get("key"), List.of(Pair.of(0L, 1L)));
        wire.getConsumed().clear();
        wire.calculate();
        assertEquals(wire.getOutOfOrderSeqs().get("key"), List.of(Pair.of(0L, 1L)));
    }

    @Test
    public void usesTheExistingConsumedRangesToResolveLateCandidates() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        add(cp, "a", 0, 0, 0, 0);
        add(cp, "b", 2, 2, 1, 1);
        add(cp, "c", 1, 1, 2, 2);
        cp.addWriteDupsOrOutOrder("key", Map.of("c", range(1, 1, 2, 2)));
        cp.calculate();
        assertEquals(cp.getOutOfOrderSeqs().get("key"), List.of(Pair.of(1L, 1L)));
    }

    @Test
    public void supportsPulsarBatchesAndScalableSegmentPositions() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        for (int i = 0; i < 3; i++) {
            long sequence = new long[] {0, 2, 1}[i];
            var position = new MessageMetadata(null, 1L, 0L, null, -1L, 1L, i, i == 0 ? 4L : 5L);
            ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
            range.setStart(new LongSeq(sequence, position));
            range.setEnd(range.getStart());
            cp.addKey("key", Map.of("range" + i, range));
        }
        cp.calculate();
        assertEquals(cp.getOutOfOrderSeqs().get("key"), List.of(Pair.of(1L, 1L)));
    }

    @Test
    public void handlesMaximumSequenceWithoutOverflowingIntervalSubtraction() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        add(cp, "a", Long.MAX_VALUE, Long.MAX_VALUE, 0, 0);
        add(cp, "b", Long.MAX_VALUE - 1, Long.MAX_VALUE, 1, 2);
        cp.calculate();
        assertEquals(cp.getOutOfOrderSeqs().get("key"),
                List.of(Pair.of(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1)));
    }

    @Test
    public void nonEmptyOrderingResultsRoundTripIncludingEmbeddedCheckpoints() throws Exception {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        cp.mergeOutOfOrderSeqs(Map.of("key", List.of(Pair.of(1L, 1L),
                Pair.of(Long.MAX_VALUE - 1, Long.MAX_VALUE))));
        var details = new ProofDetails(null, null, new Checkpoints(null, null, cp, null, cp, null, cp),
                null, null, cp.getOutOfOrderSeqs(), null);
        var restored = Util.JSON_MAPPER.readValue(Util.JSON_MAPPER.writeValueAsBytes(details), ProofDetails.class);
        assertEquals(restored.outOfOrderSeqs(), cp.getOutOfOrderSeqs());
        assertEquals(restored.checkpoints().latestConsumer().getOutOfOrderSeqs(), cp.getOutOfOrderSeqs());
        assertEquals(restored.checkpoints().verifiedConsumer().getOutOfOrderSeqs(), cp.getOutOfOrderSeqs());
        assertEquals(restored.checkpoints().failedConsumer().getOutOfOrderSeqs(), cp.getOutOfOrderSeqs());
    }

    @Test
    public void javaHttpClientReadsAnActualNonEmptyViolationResponse() throws Exception {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        add(cp, "a", 0, 0, 0, 0);
        add(cp, "b", 2, 2, 1, 1);
        add(cp, "c", 1, 1, 2, 2);
        cp.calculate();
        var details = new ProofDetails(null, null, new Checkpoints(null, null, cp, null, cp, null, cp),
                null, null, cp.getOutOfOrderSeqs(), null);
        AsyncHttpClient http = mock(AsyncHttpClient.class, RETURNS_DEEP_STUBS);
        Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn(Util.JSON_WRITER.writeValueAsString(details));
        when(http.prepareGet("http://localhost:8080/proofs/proof").execute().toCompletableFuture())
                .thenReturn(CompletableFuture.completedFuture(response));
        var restored = new CoordinatorHttpClient(http, "localhost", 8080).getProof("proof").join();
        assertEquals(restored.outOfOrderSeqs(), Map.of("key", List.of(Pair.of(1L, 1L))));
        assertEquals(restored.checkpoints().verifiedConsumer().getOutOfOrderSeqs(), restored.outOfOrderSeqs());
    }

    @Test
    public void maximumWatermarkAndConfirmedEvidenceSurviveRecalculation() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        add(cp, "high", Long.MAX_VALUE, Long.MAX_VALUE, 0L, 0L);
        add(cp, "low", Long.MAX_VALUE - 1, Long.MAX_VALUE - 1, 1L, 1L);
        cp.setOrderingWatermarks(Map.of("key", Long.MAX_VALUE));
        cp.calculate();
        assertTrue(cp.getOutOfOrderSeqs().isEmpty());
        cp.mergeOutOfOrderSeqs(Map.of("key", List.of(Pair.of(1L, 1L))));
        cp.calculate();
        assertEquals(cp.getOutOfOrderSeqs().get("key"), List.of(Pair.of(1L, 1L)));
    }

    private static void add(ConsumerCheckPoint cp, String timestamp, long start, long end, long first, long last) {
        cp.addKey("key", Map.of(timestamp, range(start, end, first, last)));
    }

    private static ConsumerCheckPoint.SeqRange range(long start, long end, long first, long last) {
        ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
        range.setStart(new LongSeq(start, new MessageMetadata(first)));
        range.setEnd(new LongSeq(end, new MessageMetadata(last)));
        return range;
    }
}
