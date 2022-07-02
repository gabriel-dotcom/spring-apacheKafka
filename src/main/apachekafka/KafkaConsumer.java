import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;

public class KafkaConsumer {
    
    @KafkaListener(topics = "topic1", groupId = "bigdataTeam")
    public void listenerGroupFoo(String msg) {
        System.out.println("Received message in group foo: " + msg);
    }

    // We can implement multiple listeners for a topic, each a differenct group id
    // @KafkaListener(topics = "topic1, topic2", groupId = "foo")

    // Spring algo supports retrieval of one or more message headers using @Header
    @KafkaListener(topics = "topic1")
    public void listenWithHeaders(
        @Payload String msg,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partion) {
            System.out.println("Received Message: " + msg
            + "from partition: " + partition);
    }

    // Consuming Messages from a specific partion
    // For a topic with multiple partions
    @KafkaListener(
        topicPartitions = @TopicPartition(
            topic = "topic1",
            partitionOffsets = {
                @PartitionOffset(partition = "0", initialOffset = "0"),
                @PartitionOffset(partition = "3", initialOffset = "3")
            }
        ),
        containerFactory = "partionsKafkaListernerContainerFactory"
    )
    public void listenerToPartition(
        @Payload String msg,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("Received message: " + msg + "from partition: " + partition);
    }

    // We can configure listernes to consume specific types of messages by adding a custom filter.
    @Bean ConcurrentKafkaListenerContainerFactory<String, String> filterKafkaListernesContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(ConsumerFactory());
        factory.setRecordFilterStrategy(
            record -> record.value().contains("Backend")
        );

        return factory;
    }

    // We can then configure a listener to use this container factory
    @KafkaListener(
        topics = "topic1",
        containerFactory = "filterKafkaListenerContainerFactory")
    public void listenWithFilter(String msg) {
        System.out.println("Received message in filtered listener: " + msg);
    }

}