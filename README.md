# kafka-consumer-lib

A high-performance, multi-threaded Kafka consumer library.

**Key Benefits:**
- **At Least Once Processing**: Manual commits guarantee data delivery
- **High Throughput**: Decoupled polling and processing for maximum performance
- **Resource Optimization**: Reduces strain on Kafka brokers
- **Vertical Scaling**: Overcomes horizontal scaling limits
- **Flexible Processing**: Handles varied processing times without complex parameter tuning
- **Backpressure Control**: Prevents memory overflow with blocking queue
- **Cost Effective**: Can run on cheaper hardware like AWS spot instances

## Features

- **Decoupled Polling and Processing**: Separates message fetching from processing for maximum efficiency
- **Manual Commits**: Ensures at-least-once processing semantics and prevents data loss
- **In-Order Processing**: Guarantees message order within partitions through pause/resume mechanism
- **Parallel Partition Processing**: Processes multiple partitions concurrently with configurable parallelism
- **Backpressure Control**: Fixed-size blocking queue prevents memory overflow and system overload
- **Graceful Rebalancing**: Implements `ConsumerRebalanceListener` for handling partition events
- **Flexible Workload Handling**: Accommodates varying processing times without `max.poll.interval.ms` tuning
- **Error Recovery**: Reset to specific offset on failure via `BatchMessageHandlerException`
- **Metrics Integration**: Built-in Datadog StatsD integration for monitoring
- **Simple Interfaces**: Easy-to-use interfaces that abstract away complexity

## Maven Dependency

```xml
<dependency>
    <groupId>com.dream11</groupId>
    <artifactId>kafka-consumer-lib</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Usage

### Basic Example

```java
import com.kafka.consumer.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Example {
    public static void main(String[] args) {
        Properties kafkaProperties = getConsumerProperties();
        List<String> topics = Collections.singletonList("test.topic");

        Consumer<String, String> consumer = new Consumer<>(
                kafkaProperties,
                topics,
                new IBatchMessageHandler<String, String>() {
                    @Override
                    public ConsumerRecord<String, String> handle(
                            List<ConsumerRecord<String, String>> records,
                            PartitionHandler<String, String> partitionHandler)
                            throws BatchMessageHandlerException {

                        // This method is called after:
                        // 1. PAUSE: Partition was paused to prevent fetching new records
                        // 2. Records were fetched and grouped by partition
                        // 3. Records were submitted to executor for parallel processing

                        try {
                            // PROCESS: Handle the batch of records from this partition
                            for (ConsumerRecord<String, String> record : records) {
                                System.out.println("[Key]:" + record.key() +
                                        " [Value]:" + record.value());
                            }

                            // Return last processed record for offset tracking
                            // RESUME: Partition will be resumed automatically after this method completes
                            return records.isEmpty() ? null : records.get(records.size() - 1);
                        } catch (Exception e) {
                            BatchMessageHandlerException exception =
                                    new BatchMessageHandlerException();
                            exception.setOffsetToReset(
                                    records.isEmpty() ? -1 : records.get(0).offset());
                            throw exception;
                        }
                    }

                    @Override
                    public void handle(ConsumerRecord<String, String> record)
                            throws BatchMessageHandlerException {
                        // Single record handler (optional)
                    }
                });

        consumer.consume();
    }

    private static Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test.group.id");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return props;
    }
}
```

### With Custom Parallelism

```java
// Process up to 8 partitions concurrently
Consumer<String, String> consumer = new Consumer<>(
    kafkaProperties,
    8,  // parallelism
    topics,
    messageHandler
);
```

### Lifecycle Flow Example

The following example demonstrates the pause/process/resume lifecycle:

```java
Consumer<String, String> consumer = new Consumer<>(
    kafkaProperties,
    topics,
    new IBatchMessageHandler<String, String>() {
        @Override
        public ConsumerRecord<String, String> handle(
            List<ConsumerRecord<String, String>> records,
            PartitionHandler<String, String> partitionHandler) 
            throws BatchMessageHandlerException {
            
            // At this point:
            // ✓ PAUSE: Partition is already paused (no new records will be fetched)
            // ✓ Records are grouped by partition
            // ✓ Processing happens in parallel thread
            
            // PROCESS: Your business logic here
            for (ConsumerRecord<String, String> record : records) {
                // Process each record
                processRecord(record);
            }
            
            // After this method returns:
            // ✓ RESUME: Partition will be automatically resumed
            // ✓ New records can be fetched for this partition
            // ✓ Offset will be committed after processing completes
            
            return records.isEmpty() ? null : records.get(records.size() - 1);
        }

        @Override
        public void handle(ConsumerRecord<String, String> record) 
            throws BatchMessageHandlerException {
            // Single record handler (optional, not used in batch mode)
        }
        
        private void processRecord(ConsumerRecord<String, String> record) {
            // Your processing logic
        }
    });

consumer.consume();  // Starts the consumer loop
```

## How It Works

The library implements a multi-threaded architecture that decouples polling from processing:

### Architecture Diagram

```
                           +---------------------+
                           |                     |
                           |     Kafka Broker    |
                           |                     |
                           |       (Kafka)       |
                           +----------+----------+
                                      |
                                      v
                       +-------------------------------+
                       |        Consumer Thread        |
                       |                               |
                       |    +--------+                 |
                       |    |  Poll  |                 |
                       |    +---+----+                 |
                       |        |                      |
                       |        v                      |
                       |  +-----------+                |
                       |  | Submit    |                |
                       |  | Tasks to  |----------------+----+
                       |  | ForkJoin  |                |    |
                       |  | Pool      |                |    |
                       |  +-----+-----+                |    |
                       |        |                      |    |
                       |        v                      |    |
                       |    +--------+                 |    |
                       |    | Commit |                 |    |
                       |    +--------+                 |    |
                       +-------------------------------+    |
                                                            |
                                                            v
                 +----------------------------------------------------+
                 |                   ForkJoin Pool                    |
                 |                                                    |
                 |   +----------------+   +----------------+          |
                 |   |   Worker 1     |   |   Worker 2     |   ...    |
                 |   | ProcessRecords |   | ProcessRecords |          |
                 |   +----------------+   +----------------+          |
                 |                                                    |
                 |   +----------------+                               |
                 |   |   Worker 3     |                               |
                 |   | ProcessRecords |                               |
                 |   +----------------+                               |
                 +----------------------------------------------------+
```

### Architecture Flow

1. **Poll**: Poll Kafka for records (default: 10ms duration)
2. **Group**: Group records by partition
3. **Create Tasks**: Create a processing task for each partition
4. **Submit**: Submit tasks to the Fork/Join Pool (BlockingQueueExecutor) for parallel execution
5. **Pause**: Pause partitions to prevent fetching new records while processing
6. **Process**: Execute business logic in parallel threads
7. **Commit**: Commit offsets for successfully processed partitions (every 5 seconds)
8. **Resume**: Resume partitions of completed tasks
9. **Repeat**: Return to polling step

### Key Design Decisions

- **Partition Pausing**: Ensures in-order processing within each partition
- **Manual Commits**: Prevents data loss and provides at-least-once guarantees
- **Blocking Queue**: Implements backpressure to prevent OOM issues
- **Per-Partition Tracking**: Maintains offset state per partition for accurate commits
- **Rebalance Handling**: Gracefully handles partition revocation and assignment events

This architecture ensures:
- ✅ No duplicate processing of records
- ✅ Controlled memory usage (no OOM)
- ✅ Proper offset management per partition
- ✅ In-order processing within partitions
- ✅ High throughput with parallel processing

## Configuration

### Required Kafka Properties

- `bootstrap.servers`: Kafka broker addresses
- `group.id`: Consumer group identifier
- `key.deserializer`: Key deserializer class
- `value.deserializer`: Value deserializer class

### Defaults

- **Parallelism**: `Runtime.getRuntime().availableProcessors()` (CPU cores)
- **Poll Duration**: 10ms
- **Commit Timeout**: 5000ms

## Error Handling

Throw `BatchMessageHandlerException` with offset to reset on failure:

```java
BatchMessageHandlerException exception = new BatchMessageHandlerException();
exception.setOffsetToReset(failedRecord.offset());
        throw exception;
```

The consumer will seek to the specified offset and retry from that point.

## Metrics

Metrics infrastructure is available via `KafkaMetricsManager` (Datadog StatsD, default: `localhost:8125`).

Set `DD_AGENT_HOST` environment variable to configure StatsD host.

Available metric methods in `KafkaMetricsManager`:
- `sendPartitionSizeCountMetric(int partition, int size, String topic)`: Partition batch size
- `sendPartitionRevokedCountMetric(Collection<TopicPartition> partitions)`: Partition revocation count
- `sendPartitionPartitionAssignedCountMetric(Collection<TopicPartition> partitions)`: Partition assignment count

Note: Metrics manager is created automatically but metric methods are not called automatically. Extend the Consumer class to add metric calls as needed.

## Graceful Shutdown

```java
consumer.close();  // Stops consumer, wakes up poll loop, closes metrics manager
// The consume() method's finally block handles task completion and offset commits
```

## Architecture Details

### Consumer Loop

The main `consume()` method implements the following loop:

```java
while (!stopped.get()) {
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(10));  // 1. Poll
    handleFetchedRecords(records);  // 2. Group, create tasks, submit, pause
    checkActiveTasks();  // 3. Check completion, handle failures, resume
    commitOffsets();  // 4. Commit offsets periodically
}
```

### Rebalancing

The library implements `ConsumerRebalanceListener` to handle:
- **Partition Revocation**: Stops active tasks, waits for completion, commits offsets
- **Partition Assignment**: Resumes partitions for new assignments
- **Partition Loss**: Cleans up lost partitions

### Error Handling

When a `RejectedExecutionException` occurs (queue full):
- The partition handler seeks back to the first offset of the batch
- Prevents memory overflow by rejecting excessive messages
- Logs a warning to increase parallelism if needed

## Repository

Open source repository: https://github.com/dream11/kafka-consumer-library

## License

MIT License
