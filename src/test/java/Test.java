import com.kafka.consumer.BatchMessageHandlerException;
import com.kafka.consumer.Consumer;
import com.kafka.consumer.IBatchMessageHandler;
import com.kafka.consumer.PartitionHandler;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Test {

  public static void main(String[] args) {

    // Kafka consumer properties you want to submit
    Properties kafkaProperties = getConsumerProperties();

    // Parallelism to process records. Each partition will be processed by a separate threads.
    // If parallelism is set to 10 and number of partitions assigned to this consumer is 50
    // then 10 threads will be used to process 50 partitions in parallel.
    // Each partition will be processed bya single thread.
    int parallelism = 5;

    // list of topics to subscribe to
    List<String> topicList = Collections.singletonList("test");

    Consumer<String, String> consumer =
        new Consumer<>(
            kafkaProperties,
            parallelism,
            topicList,
            new IBatchMessageHandler<String, String>() {
              @Override
              public ConsumerRecord<String, String> handle(
                  List<ConsumerRecord<String, String>> partitionRecords,
                  PartitionHandler<String, String> partitionHandler)
                  throws BatchMessageHandlerException {
                try {

                  for (int i = 0; i < partitionRecords.size(); i++) {
                    System.out.println(
                        "[Key]:"
                            + partitionRecords.get(i).key()
                            + " [Value]:"
                            + partitionRecords.get(i).value());
                  }
                  return partitionRecords.get(partitionRecords.size() - 1);
                } catch (Exception e) {
                  BatchMessageHandlerException exception = new BatchMessageHandlerException();
                  exception.setOffsetToReset(partitionRecords.get(0).offset());
                  throw exception;
                }
              }

              @Override
              public void handle(ConsumerRecord<String, String> partitionRecord)
                  throws BatchMessageHandlerException {
                System.out.println("Successfully processed");
              }
            });
    consumer.consume();
  }

  private static Properties getConsumerProperties() {
    final Properties consumerProperties = new Properties();
    consumerProperties.put("bootstrap.servers", "localhost:9092");
    consumerProperties.put("group.id", "test.group.id.v1");
    consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
    consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
    consumerProperties.put("auto.offset.reset", "earliest");
    return consumerProperties;
  }

  /**
   * @param partitionHandler - List of records fetched from a partition.
   * @throws BatchMessageHandlerException - Should throw BatchMessageHandlerException with offset it
   *     wants to seek on error
   */
  private static ConsumerRecord batchHandler(List<ConsumerRecord> partitionHandler)
      throws BatchMessageHandlerException {
    try {

      for (int i = 0; i < partitionHandler.size(); i++) {
        System.out.println(
            "[Key]:"
                + partitionHandler.get(i).key()
                + " [Value]:"
                + partitionHandler.get(i).value());
      }
      return partitionHandler.get(partitionHandler.size() - 1);
    } catch (Exception e) {
      BatchMessageHandlerException exception = new BatchMessageHandlerException();
      exception.setOffsetToReset(partitionHandler.get(0).offset());
      throw exception;
    }
  }
}
