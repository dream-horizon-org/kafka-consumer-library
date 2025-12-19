package com.kafka.consumer;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface IBatchMessageHandler<K, V> {
  /**
   * @param partitionHandler - Handler to process records of a partition in batch
   * @throws BatchMessageHandlerException - Throws BatchMessageHandlerException with offset to reset
   *     to for that partition
   */
  ConsumerRecord<K, V> handle(
      List<ConsumerRecord<K, V>> partitionRecords, PartitionHandler<K, V> partitionHandler)
      throws BatchMessageHandlerException;

  void handle(ConsumerRecord<K, V> partitionRecord) throws BatchMessageHandlerException;
}
