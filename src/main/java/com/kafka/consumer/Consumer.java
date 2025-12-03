package com.kafka.consumer;

import com.kafka.consumer.metrics.KafkaMetricsManager;
import com.kafka.consumer.utils.BlockingQueueExecutor;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

@Slf4j
public class Consumer<K, V> implements IConsumer, ConsumerRebalanceListener {

  public static final int DEFAULT_POLL_DURATION = 10;
  private static final int DEFAULT_PARALLELISM = Runtime.getRuntime().availableProcessors();

  private static final int COMMIT_TIMEOUT = 5000;
  private final Properties properties;
  private final ExecutorService executor;
  private final List<String> topics;
  private final IBatchMessageHandler<K, V> messageHandler;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final Map<TopicPartition, PartitionHandler<K, V>> activeTasks = new HashMap<>();
  private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
  KafkaMetricsManager metricsManager;
  private KafkaConsumer<K, V> consumer = null;

  private long lastCommitTime = System.currentTimeMillis();

  /**
   * @param kafkaProperties - Consumer Kafka properties to set when consuming
   * @param parallelism - Number of partitions to be processed in parallel, Default equal to number
   *     of cpu cores * 2 to achieve maximum throughput.
   * @param topics- Topics to subscribe to
   * @param batchMessageHandler - BatchHandler function to process records. Should throw exception
   *     BatchMessageHandlerException with offset to seek on failure.
   */
  public Consumer(
      Properties kafkaProperties,
      int parallelism,
      List<String> topics,
      IBatchMessageHandler<K, V> batchMessageHandler) {

    this.properties = kafkaProperties;
    this.topics = topics;
    this.messageHandler = batchMessageHandler;
    this.executor =
        new BlockingQueueExecutor((parallelism <= 0) ? DEFAULT_PARALLELISM : parallelism);
    this.metricsManager =
        new KafkaMetricsManager(
            Optional.ofNullable(properties.getProperty("group.id"))
                .orElse("default-consumer-group"));
  }

  /**
   * @param kafkaProperties - Consumer Kafka properties to set when consuming
   * @param topics- Topics to subscribe to
   * @param batchMessageHandler - BatchHandler function to process records. Should throw exception
   *     BatchMessageHandlerException with offset to seek on failure.
   */
  public Consumer(
      Properties kafkaProperties,
      List<String> topics,
      IBatchMessageHandler<K, V> batchMessageHandler) {
    this(kafkaProperties, DEFAULT_PARALLELISM, topics, batchMessageHandler);

    if (topics.isEmpty()) {
      log.error("Topics list is empty");
    }

    if (Objects.isNull(messageHandler)) {
      log.error("No Batch MessageHandler present");
    }
  }

  public void consume() {
    try {
      consumer = new KafkaConsumer<>(properties);
      consumer.subscribe(topics, this);
      log.info("Subscribed to topics {} ", topics);
      while (!stopped.get()) {
        ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(DEFAULT_POLL_DURATION));
        handleFetchedRecords(records);
        checkActiveTasks();
        commitOffsets();
      }
    } catch (WakeupException e) {
      // Ignore exception if stopped
      if (!stopped.get()) throw e;
    } catch (Exception exception) {
      // This is possible if there is any exception in ConsumerRebalanceListener
      log.error("Got Exception from multithreaded consumer run: ", exception);
      throw exception;
    } finally {
      log.info("Multithreaded consumer finally called");
      consumer.close();
      executor.shutdown();
    }
  }

  /**
   * This method is used to handle the fetched records from the consumer.poll() method. It will
   * submit the records to the executor service for processing.
   *
   * @param records - ConsumerRecords fetched from the consumer.poll() method
   */
  private void handleFetchedRecords(ConsumerRecords<K, V> records) {
    if (!records.isEmpty()) {
      log.info("all topics message count in poll: {}", records.count());
      List<TopicPartition> partitionsToPause = new ArrayList<>();
      records
          .partitions()
          .forEach(
              partition -> {
                List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
                PartitionHandler<K, V> handler = null;
                try {
                  handler = new PartitionHandler<>(messageHandler, partition, partitionRecords);
                  executor.submit(handler);
                  partitionsToPause.add(partition);
                  activeTasks.put(partition, handler);
                } catch (RejectedExecutionException ex) {
                  log.warn(
                      "Excessive messages triggered for {}. Please increase consumers",
                      partition.topic());
                  /*
                  When we try to submit a runnable to an executor service which has
                  exceeded the number of runnables that can be occupied by the queue of the thread pool
                  we have defined, then it will trigger an RejectedExecutionException. For all this partition's
                  records, we will have to resume the polling for this partition and seek to the original offset
                   */
                  if (Objects.nonNull(handler))
                    handleRejectedRecordsForPartition(handler, partition);
                }
              });
      consumer.pause(partitionsToPause);
    }
  }

  /**
   * This method is used to check the active tasks and remove the finished tasks from the active
   * tasks and commit the offsets for the finished tasks.
   */
  private void checkActiveTasks() {
    List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
    Map<TopicPartition, Long> failedTasksPartitions = new HashMap<>();
    activeTasks.forEach(
        (partition, task) -> {
          if (task.isFinished()) finishedTasksPartitions.add(partition);
          if (task.isFailed()) failedTasksPartitions.put(partition, task.getOffsetToSeek());
          long offset = task.getCurrentOffset();
          if (offset > 0) offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });
    finishedTasksPartitions.forEach(activeTasks::remove);
    failedTasksPartitions.forEach(consumer::seek);
    consumer.resume(finishedTasksPartitions);
  }

  /**
   * This method is used to handle the rejected records for a partition. It will seek to the first
   * offset of the partition.
   *
   * @param handler - PartitionHandler for the partition
   * @param topicPartition - TopicPartition for the partition
   */
  private void handleRejectedRecordsForPartition(
      PartitionHandler<K, V> handler, TopicPartition topicPartition) {
    consumer.seek(topicPartition, handler.getFirstOffset());
  }

  /** This method is used to commit the offsets for the partitions. */
  private void commitOffsets() {
    try {
      long currentTimeMillis = System.currentTimeMillis();
      if (currentTimeMillis - lastCommitTime > COMMIT_TIMEOUT) {
        if (!offsetsToCommit.isEmpty()) {
          consumer.commitSync(offsetsToCommit);
          offsetsToCommit.clear();
        }
        lastCommitTime = currentTimeMillis;
      }
    } catch (Exception e) {
      log.error("Failed to commit offsets!", e);
    }
  }

  /**
   * This method is used to handle the partitions revoked event. It will stop all the tasks handling
   * records from revoked partitions, wait for the stopped tasks to complete processing of the
   * current record, collect offsets for revoked partitions and commit offsets for revoked
   * partitions.
   *
   * @param partitions - Collection of TopicPartitions which are revoked
   */
  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    log.info("onPartitionsRevoked called");
    log.info("Stopping all tasks handling records from revoked partitions");
    // 1. Stop all tasks handling records from revoked partitions
    Map<TopicPartition, PartitionHandler<K, V>> stoppedTask = new HashMap<>();
    for (TopicPartition partition : partitions) {
      PartitionHandler<K, V> partitionHandler = activeTasks.remove(partition);
      if (Objects.nonNull(partitionHandler)) {
        partitionHandler.stop();
        stoppedTask.put(partition, partitionHandler);
      }
    }

    log.info("Waiting for stopped tasks to complete processing of current record");
    // 2. Wait for stopped tasks to complete processing of current record
    stoppedTask.forEach(
        (partition, partitionHandler) -> {
          long offset = partitionHandler.waitForCompletion();
          if (offset > 0) offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });

    log.info("Collecting offsets for revoked partitions");
    // 3. collect offsets for revoked partitions
    Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
    partitions.forEach(
        partition -> {
          OffsetAndMetadata offset = offsetsToCommit.remove(partition);
          if (offset != null) revokedPartitionOffsets.put(partition, offset);
        });

    log.info("committing offsets for revoked partitions");
    // 4. commit offsets for revoked partitions
    try {
      consumer.commitSync(revokedPartitionOffsets);
    } catch (Exception e) {
      log.warn("Failed to commit offsets for revoked partitions: " + e.getMessage());
    }
    log.info("onPartitionsRevoked finished");
  }

  /**
   * This method is used to handle the partitions assigned event. It will resume the partitions.
   *
   * @param partitions - Collection of TopicPartitions which are assigned
   */
  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    log.info("onPartitionsAssigned called");
    if (partitions.size() > 0) consumer.resume(partitions);
    log.info("onPartitionsAssigned finished");
  }

  /**
   * This method is used to handle the partitions lost event. It will stop all the tasks handling
   * records from lost partitions.
   *
   * @param partitions - Collection of TopicPartitions which are lost
   */
  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    log.info("onPartitionsLost called");
    log.info("Stopping all tasks handling records from lost partitions");
    // 1. Stop all tasks handling records from lost partitions
    for (TopicPartition partition : partitions) {
      offsetsToCommit.remove(partition);
      PartitionHandler<K, V> partitionHandler = activeTasks.remove(partition);
      if (Objects.nonNull(partitionHandler)) {
        partitionHandler.stop();
      }
    }
  }

  /** This method is used to close the consumer and stop the consumer. */
  public void close() {
    metricsManager.close();
    stopped.set(true);
    consumer.wakeup();
  }
}
