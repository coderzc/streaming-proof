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

/** Protocol evidence supplied by the driver, never inferred from worker wall clocks. */
public record DeliveryContext(Scope scope, boolean redelivery, boolean replayProvenanceKnown) {
    public enum Scope { PARTITION, PARTITION_LOG, KEY }

    public static DeliveryContext kafka() {
        return new DeliveryContext(Scope.PARTITION, false, true);
    }

    public static DeliveryContext pulsar(boolean redelivery) {
        return new DeliveryContext(Scope.KEY, redelivery, true);
    }

    public static DeliveryContext pulsarPartition(boolean redelivery) {
        return new DeliveryContext(Scope.PARTITION_LOG, redelivery, true);
    }

    public static DeliveryContext checkpoint() {
        return new DeliveryContext(Scope.KEY, false, false);
    }
}
