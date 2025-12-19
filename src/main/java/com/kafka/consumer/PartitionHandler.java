package com.kafka.consumer;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

@Slf4j
@NoArgsConstructor
public class PartitionHandler<K, V> implements Runnable {
  /**
   * When an object implementing interface <code>Runnable</code> is used to create a thread,
   * starting the thread causes the object's <code>run</code> method to be called in that separately
   * executing thread.
   *
   * <p>The general contract of the method <code>run</code> is that it may take any action
   * whatsoever.
   *
   * @see Thread#run()
   */
  private IBatchMessageHandler<K, V> messageHandler;

  private TopicPartition topicPartition;
  private List<ConsumerRecord<K, V>> partitionRecords;
  private long firstOffset;
  private long offsetToSeek;
  private volatile boolean stopped = false;
  private volatile boolean started = false;
  private volatile boolean finished = false;
  private volatile boolean failed = false;
  private final CompletableFuture<Long> completion = new CompletableFuture<>();
  private final ReentrantLock startStopLock = new ReentrantLock();
  private final AtomicLong currentOffset = new AtomicLong();

  public PartitionHandler(
      IBatchMessageHandler<K, V> messageHandler,
      TopicPartition topicPartition,
      List<ConsumerRecord<K, V>> partitionRecords) {
    this.messageHandler = messageHandler;
    this.partitionRecords = partitionRecords;
    this.topicPartition = topicPartition;
    this.firstOffset = partitionRecords.size() > 0 ? partitionRecords.get(0).offset() : null;
  }

  @Override
  public void run() {
    handlePartition(partitionRecords);
  }

  private void handlePartition(List<ConsumerRecord<K, V>> partitionRecords) {
    if (partitionRecords.size() == 0) return;
    startStopLock.lock();
    if (stopped) {
      return;
    }
    try {
      started = true;
      startStopLock.unlock();
      if (Objects.nonNull(messageHandler)) {
        ConsumerRecord<K, V> lastRecord = messageHandler.handle(partitionRecords, this);
        if (Objects.nonNull(lastRecord)) currentOffset.set(lastRecord.offset() + 1);
      } else {
        log.error("message processor found null");
      }
    } catch (BatchMessageHandlerException emx) {
      log.error("Exception in task run: ", emx);
      if (emx.getOffsetToReset() > 0) {
        offsetToSeek = emx.getOffsetToReset();
        currentOffset.set(offsetToSeek);
      } else offsetToSeek = firstOffset;
      failed = true;
    } catch (Exception ex) {
      log.error("Exception in task run: ", ex);
      offsetToSeek = firstOffset;
      failed = true;
    } finally {
      finished = true;
      completion.complete(currentOffset.get());
    }
  }

  public void stop() {
    startStopLock.lock();
    this.stopped = true;
    if (!started) {
      finished = true;
      completion.complete(currentOffset.get());
    }
    startStopLock.unlock();
  }

  public long waitForCompletion() {
    try {
      return completion.get();
    } catch (InterruptedException | ExecutionException e) {
      log.warn("error while waiting for task completion", e);
      return -1;
    }
  }

  public boolean isFinished() {
    return finished;
  }

  public long getCurrentOffset() {
    return currentOffset.get();
  }

  public boolean isFailed() {
    return failed;
  }

  public long getOffsetToSeek() {
    return offsetToSeek;
  }

  public long getFirstOffset() {
    return firstOffset;
  }

  public boolean isStopped() {
    return stopped;
  }
}
