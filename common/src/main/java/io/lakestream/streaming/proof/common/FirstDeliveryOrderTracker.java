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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.Pair;

/** First-delivery ordering evidence for one consumer, independent of watermark trimming. */
public final class FirstDeliveryOrderTracker {
    private final Map<String, State> keys = new HashMap<>();

    /** Record an arrival, ignoring sequences already delivered to this consumer. */
    public synchronized void observe(String key, long sequence) {
        if (sequence < 0) {
            throw new IllegalArgumentException("Sequence must not be negative");
        }
        State state = keys.computeIfAbsent(key, ignored -> new State());
        Map.Entry<Long, Long> floor = state.seen.floorEntry(sequence);
        if (floor != null && floor.getValue() >= sequence) {
            return;
        }
        if (sequence < state.maximum) {
            add(state.violations, sequence);
        }
        add(state.seen, sequence);
        state.maximum = Math.max(state.maximum, sequence);
    }

    /** Return a detached snapshot of inclusive violation ranges. */
    public synchronized Map<String, List<Pair<Long, Long>>> violations() {
        Map<String, List<Pair<Long, Long>>> snapshot = new HashMap<>();
        keys.forEach((key, state) -> {
            if (!state.violations.isEmpty()) {
                List<Pair<Long, Long>> ranges = new ArrayList<>();
                state.violations.forEach((start, end) -> ranges.add(Pair.of(start, end)));
                snapshot.put(key, ranges);
            }
        });
        return snapshot;
    }

    private static void add(TreeMap<Long, Long> ranges, long sequence) {
        long start = sequence;
        long end = sequence;
        Map.Entry<Long, Long> left = ranges.floorEntry(sequence);
        if (left != null) {
            if (left.getValue() >= sequence) {
                return;
            }
            if (left.getValue() == sequence - 1) {
                start = left.getKey();
                ranges.remove(left.getKey());
            }
        }
        Map.Entry<Long, Long> right = ranges.ceilingEntry(sequence);
        if (right != null && sequence != Long.MAX_VALUE && right.getKey() == sequence + 1) {
            end = right.getValue();
            ranges.remove(right.getKey());
        }
        ranges.put(start, end);
    }

    private static final class State {
        private final TreeMap<Long, Long> seen = new TreeMap<>();
        private final TreeMap<Long, Long> violations = new TreeMap<>();
        private long maximum = -1;
    }
}
