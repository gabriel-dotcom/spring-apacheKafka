import javax.websocket.SendResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.concurrent.ListenableFutureCallbackRegistry;

// Publishing Messages
public class KafkaTemplate {
    
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String msg) {
        
        /* The send API returns a ListenableFuture object. If we want to block the sending thread and get the result 
        about the sent message, we can call the get API of the ListenableFuture object. The thread will wait for the result,
        but it will slow down the producer. 

        Kafka is a fast stream processing platform. Therefore, it's better to handle the results asynchronously so that the 
        subsequent messages do not wait for the result of the previous message.
        */

        ListenableFuture<SendResult<String, String>> future = KafkaTemplate.send(topic1, msg);

        future.addCallback(new ListenableFutureCallbackRegistry<SendResult<String, String>>() {
            
            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Send message=[" + msg + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=[" + msg + "] due to : " + ex.getMessage());
            }

        });

    }

}
