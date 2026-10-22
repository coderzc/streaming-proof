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
import static org.testng.Assert.assertTrue;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

public class FirstDeliveryOrderTrackerTest {
    @Test
    public void ignoresReplayedMessagesButDetectsFirstDeliveryInversion() {
        FirstDeliveryOrderTracker tracker = new FirstDeliveryOrderTracker();
        for (long seq : new long[] {0, 2, 0, 2, 1, 1, 3}) {
            tracker.observe("key", seq);
        }
        assertEquals(tracker.violations().get("key"), List.of(Pair.of(1L, 1L)));
    }

    @Test
    public void sequentialReadsAndLaterReplaysDoNotCreateViolations() {
        FirstDeliveryOrderTracker tracker = new FirstDeliveryOrderTracker();
        for (long seq = 0; seq < 10000; seq++) {
            tracker.observe("key", seq);
        }
        tracker.observe("key", 0);
        tracker.observe("key", 9999);
        assertTrue(tracker.violations().isEmpty());
    }

    @Test
    public void compactsAdjacentViolationsAndKeepsKeysIndependent() {
        FirstDeliveryOrderTracker tracker = new FirstDeliveryOrderTracker();
        tracker.observe("a", 5);
        tracker.observe("a", 2);
        tracker.observe("a", 1);
        tracker.observe("a", 3);
        tracker.observe("b", 0);
        tracker.observe("b", 1);
        assertEquals(tracker.violations().get("a"), List.of(Pair.of(1L, 3L)));
        assertEquals(tracker.violations().size(), 1);
        tracker.violations().clear();
        assertEquals(tracker.violations().size(), 1);
    }

    @Test
    public void handlesMaximumSequenceWithoutOverflowingAdjacencyChecks() {
        FirstDeliveryOrderTracker tracker = new FirstDeliveryOrderTracker();
        tracker.observe("key", Long.MAX_VALUE);
        tracker.observe("key", Long.MAX_VALUE - 1);
        tracker.observe("key", Long.MAX_VALUE);
        assertEquals(tracker.violations().get("key"),
                List.of(Pair.of(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1)));
    }
}
