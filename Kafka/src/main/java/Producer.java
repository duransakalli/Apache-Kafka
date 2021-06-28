import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Date;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        String clientId = "my-producer";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");
        props.put("client.id", clientId);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int numRecords = 100;
        String topic = "numbers";
        try {
            for (int i = 0; i < numRecords; i++) {
                String message = String.format("Producer %s has sent message %s at %s", clientId, i, new Date());
                System.out.println(message);
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), message));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
