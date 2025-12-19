package com.kafka.consumer;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Data
public class BatchMessageHandlerException extends Exception {
  /** Points to the failed record from a Batch */
  private ConsumerRecord failedRecord;
  /** Points to the offset to seek to when processing in a Batch. */
  private long offsetToReset;
}
