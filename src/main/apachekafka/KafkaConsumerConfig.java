import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;

// Consumer configuration
// For consuming messages, we need to configure a ConsumerFactory and a KafkaListenerContainerFactory.
// Once these beans are available in the Spring bean factory, POJO-based consumers can be configured using @KafkaListener annotation.
@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, String> props = new HashMap<>();
        props.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapAddress
        );
        props.put(
            ConsumerConfig.GROUP_ID_CONFIG,
            groupId
        );
        props.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class
        );
        props.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class
        );

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

}
