# kafka-consumer-lib

Multi-threaded Kafka consumer library with parallel partition processing, batch handling, and built-in metrics.

## Features

- **Parallel Processing**: Processes partitions concurrently with configurable parallelism
- **Pause/Resume**: Automatically pauses partitions during processing, resumes when complete
- **Batch Handling**: Groups records by partition for efficient batch processing
- **Backpressure**: Blocking queue prevents system overload
- **Offset Management**: Per-partition offset tracking and incremental commits
- **Rebalancing**: Handles partition rebalancing with graceful task completion
- **Error Recovery**: Reset to specific offset on failure via `BatchMessageHandlerException`
- **Metrics**: Built-in Datadog StatsD integration

## Maven Dependency

```xml
<dependency>
    <groupId>com.dream11</groupId>
    <artifactId>kafka-consumer-lib</artifactId>
    <version>1.0.2-SNAPSHOT</version>
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

The library automatically manages partition pause/resume:

1. **Pause**: When records are fetched for a partition, the partition is paused to prevent fetching new records
2. **Process**: Records are submitted to executor for parallel processing
3. **Resume**: Partition is resumed once processing completes, allowing new records to be fetched

This ensures:
- No duplicate processing of records
- Controlled memory usage
- Proper offset management per partition

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

## License

MIT License
