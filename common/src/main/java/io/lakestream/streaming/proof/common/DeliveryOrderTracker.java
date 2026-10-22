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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.Pair;

/** Consumer-local protocol order, scoped by partition or key, with cumulative evidence. */
public final class DeliveryOrderTracker {
    private final Map<Integer, Long> offsets = new HashMap<>();
    private final Set<Integer> assigned = new HashSet<>();
    private final Map<String, MessageMetadata> positions = new HashMap<>();
    private final Map<String, TreeMap<Long, Long>> seen = new HashMap<>();
    private final Map<String, TreeMap<Long, Long>> violations = new HashMap<>();
    private final Set<String> gaps = new HashSet<>();
    private long observations;

    public synchronized void assigned(int partition) {
        // Retained cooperative assignments keep their existing read position.
        if (assigned.add(partition)) {
            offsets.remove(partition);
        }
    }

    public synchronized void revoked(int partition) {
        assigned.remove(partition);
        offsets.remove(partition);
    }

    public synchronized void observe(String key, long sequence, MessageMetadata metadata, DeliveryContext context) {
        if (metadata == null || context == null) {
            gaps.add("Driver did not provide protocol ordering evidence");
            return;
        }
        if (context.scope() == DeliveryContext.Scope.PARTITION) {
            Integer partition = metadata.partition();
            Long offset = metadata.offset();
            if (partition == null || offset == null || offset < 0 || !assigned.contains(partition)) {
                gaps.add("Kafka record lacks a valid active partition assignment/offset");
                return;
            }
            observations++;
            Long previous = offsets.get(partition);
            offsets.merge(partition, offset, Math::max);
            if (previous != null && offset <= previous) {
                add(violations.computeIfAbsent("partition:" + partition, ignored -> new TreeMap<>()), offset);
            }
            return;
        }
        if ((context.scope() != DeliveryContext.Scope.KEY && context.scope() != DeliveryContext.Scope.PARTITION_LOG)
                || metadata.ledgerId() == null || metadata.entryId() == null
                || metadata.ledgerId() < 0 || metadata.entryId() < 0 || sequence < 0 || key == null) {
            gaps.add("Driver provided an unsupported ordering position");
            return;
        }
        observations++;
        String scope = "partition:" + metadata.partition() + ":key:" + key;
        TreeMap<Long, Long> delivered = seen.computeIfAbsent(scope, ignored -> new TreeMap<>());
        Map.Entry<Long, Long> floor = delivered.floorEntry(sequence);
        if (floor != null && floor.getValue() >= sequence) {
            return; // A known sequence is replay or a repeated publication, not a new delivery inversion.
        }
        add(delivered, sequence);
        if (context.redelivery()) {
            return; // Redelivery must not move the baseline for fresh messages.
        }
        String positionScope = context.scope() == DeliveryContext.Scope.PARTITION_LOG
                ? "partition:" + metadata.partition() : scope;
        MessageMetadata previous = positions.get(positionScope);
        if (previous != null && !metadata.isAfter(previous) && !context.redelivery()) {
            if (context.replayProvenanceKnown()) {
                add(violations.computeIfAbsent(scope, ignored -> new TreeMap<>()), sequence);
            } else {
                gaps.add("Checkpoint replay/ownership provenance is unavailable for a backward read");
            }
        }
        if (previous == null || metadata.isAfter(previous)) {
            positions.put(positionScope, metadata);
        }
    }

    public synchronized Map<String, List<Pair<Long, Long>>> violations() {
        Map<String, List<Pair<Long, Long>>> result = new HashMap<>();
        violations.forEach((scope, ranges) -> {
            List<Pair<Long, Long>> values = new ArrayList<>();
            ranges.forEach((start, end) -> values.add(Pair.of(start, end)));
            result.put(scope, values);
        });
        return result;
    }

    public synchronized Set<String> gaps() {
        return Set.copyOf(gaps);
    }

    public synchronized long observations() {
        return observations;
    }

    private static void add(TreeMap<Long, Long> ranges, long value) {
        long start = value;
        long end = value;
        Map.Entry<Long, Long> left = ranges.floorEntry(value);
        if (left != null) {
            if (left.getValue() >= value) {
                return;
            }
            if (left.getValue() != Long.MAX_VALUE && left.getValue() + 1 == value) {
                start = left.getKey();
                ranges.remove(left.getKey());
            }
        }
        Map.Entry<Long, Long> right = ranges.ceilingEntry(value);
        if (right != null && value != Long.MAX_VALUE && right.getKey() == value + 1) {
            end = right.getValue();
            ranges.remove(right.getKey());
        }
        ranges.put(start, end);
    }
}
