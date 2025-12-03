# kafka-consumer-lib

##### Usage

```java
public class Test {

    public static void main(String[] args) {

        //Kafka consumer properties you want to submit
        Properties kafkaProperties = getConsumerProperties();

        // list of topics to subscribe to
        List<String> topicList = Collections.singletonList("test.topic");

        Consumer<String, String> consumer = new Consumer<>(
                kafkaProperties,
                topicList,
                new IBatchMessageHandler<String, String>() {
                    @Override
                    public ConsumerRecord<String, String> handle(List<ConsumerRecord<String, String>> list, PartitionHandler<String, String> partitionHandler) throws BatchMessageHandlerException {
                        try {
                            for (int i = 0; i < partitionHandler.size(); i++) {
                                System.out.println(
                                        "[Key]:" + partitionHandler.get(i).key() +
                                                " [Value]:" + partitionHandler.get(i).value());
                            }
                        } catch (Exception e) {
                            BatchMessageHandlerException exception = new BatchMessageHandlerException();
                            exception.setOffsetToReset(partitionHandler.get(0).offset());
                            throw exception;
                        }
                    }

                    @Override
                    public void handle(ConsumerRecord<String, String> consumerRecord) throws BatchMessageHandlerException {
                    }
                });
        consumer.consume();

    }

    private static Properties getConsumerProperties() {
        final Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", "localhost:9092");
        consumerProperties.put("group.id", "test.group.id");
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
        return consumerProperties;
    }
}
