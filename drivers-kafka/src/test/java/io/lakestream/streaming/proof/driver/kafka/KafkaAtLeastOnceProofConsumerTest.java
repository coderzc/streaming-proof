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
package io.lakestream.streaming.proof.driver.kafka;

import io.lakestream.streaming.proof.common.MessageListener;
import io.lakestream.streaming.proof.common.ProofValue;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
public class KafkaAtLeastOnceProofConsumerTest {

    @Mock
    private MessageListener messageListener;
    
    @Mock
    private KafkaConsumer<String, ProofValue> mockConsumer;
    
    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test(timeOut = 10000)
    public void passesPartitionPositionAndProtocolContextToTheVerifier() throws Exception {
        var partition = new org.apache.kafka.common.TopicPartition("topic", 2);
        var record = new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                "topic", 2, 100L, "key", ProofValue.of(1L));
        var records = new org.apache.kafka.clients.consumer.ConsumerRecords<>(Map.of(partition, java.util.List.of(record)));
        Mockito.when(mockConsumer.poll(Mockito.any(java.time.Duration.class)))
                .thenReturn(records).thenAnswer(invocation -> {
                    new java.util.concurrent.CountDownLatch(1).await();
                    return null;
                });
        KafkaAtLeastOnceProofConsumer consumer = new KafkaAtLeastOnceProofConsumer(
                "consumer", mockConsumer, Map.of(), 0, messageListener);
        try {
            Mockito.verify(messageListener, Mockito.timeout(3000)).onMessage(
                    Mockito.eq("key"), Mockito.eq(1L),
                    Mockito.argThat(metadata -> metadata.partition() == 2 && metadata.offset() == 100),
                    Mockito.eq(io.lakestream.streaming.proof.common.DeliveryContext.kafka()));
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testClose() throws Exception {
        // Arrange
        String consumerName = "test-consumer";
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("group.id", "test-group");
        
        KafkaAtLeastOnceProofConsumer consumer = new KafkaAtLeastOnceProofConsumer(
                consumerName,
                mockConsumer,
                configs,
                0L, // No consume delay
                messageListener);
        
        // Act
        consumer.close();
        
        // Assert
        Mockito.verify(mockConsumer).close(); // Verify consumer.close() was called
    }
}