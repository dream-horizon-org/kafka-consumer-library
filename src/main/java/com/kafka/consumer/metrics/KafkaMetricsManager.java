package com.kafka.consumer.metrics;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import java.util.Collection;
import org.apache.kafka.common.TopicPartition;

public class KafkaMetricsManager {
  private NonBlockingStatsDClient Statsd;
  private String consumerGroup;

  public KafkaMetricsManager(String consumerGroup) {
    Statsd =
        new NonBlockingStatsDClientBuilder()
            .prefix("kafka-consumer")
            .hostname(System.getenv().getOrDefault("DD_AGENT_HOST", "localhost"))
            .port(8125)
            .build();
    this.consumerGroup = consumerGroup;
  }

  public void close() {
    Statsd.close();
  }

  public void sendPartitionSizeCountMetric(int partition, int size, String topic) {
    Statsd.count(
        "kafka.consumer.partition.size",
        partition,
        "partition:" + partition,
        "consumer_group:" + consumerGroup,
        "topic:" + topic);
  }

  public void sendPartitionRevokedCountMetric(Collection<TopicPartition> partitions) {
    partitions.forEach(
        topicPartition -> {
          Statsd.increment(
              "kafka.consumer.partition.revoked",
              "partition:" + topicPartition.partition(),
              "consumer_group:" + consumerGroup,
              "topic:" + topicPartition.topic());
        });
  }

  public void sendPartitionPartitionAssignedCountMetric(Collection<TopicPartition> partitions) {
    partitions.forEach(
        topicPartition -> {
          Statsd.increment(
              "kafka.consumer.partition.assigned",
              "partition:" + topicPartition.partition(),
              "consumer_group:" + consumerGroup,
              "topic:" + topicPartition.topic());
        });
  }
}
