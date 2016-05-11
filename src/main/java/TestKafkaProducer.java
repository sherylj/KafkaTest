import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * Created by sherylj on 12/7/15.
 */
public class TestKafkaProducer {

    public static void main(String[] args) {
        if (args.length != 2)
        {

            System.out.println("Usage: TestKafkaProducer <broker list> <zookeeper>");
            System.exit(-1);
        }


        // long events = Long.parseLong(args[0]);
        Random rnd = new Random();

        Properties props = new Properties();
        System.out.println(args[0]);
        System.out.println(args[1]);

        props.put("bootstrap.servers", args[0]);
        props.put("metadata.broker.list", args[0]);
        props.put("zk.connect", args[1]);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.required.acks", "1");

        String TOPIC = "testjava";
        String data = "Hi! This is a message from the vega galaxy";

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for(int i = 0; i < 4; i++) {
            producer.send(new ProducerRecord<String, String>(TOPIC, data));
        }

        System.out.println("This is sent");

        producer.close();


    }
}
