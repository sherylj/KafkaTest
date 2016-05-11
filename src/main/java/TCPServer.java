import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.net.*;
import java.util.*;

class TCPServer
{
    public static void main(String args[]) throws Exception{
        if (args.length != 2)
        {

            System.out.println("Usage: TestKafkaProducer <broker list> <zookeeper>");
            System.exit(-1);
        }

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
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        ServerSocket socket = new ServerSocket(6789);
        StringBuilder record = new StringBuilder();
        String line = null;

        while(true){
            Socket connectionSocket = socket.accept();
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));

            while ((line = inFromClient.readLine()) != null){
                record.append(line);
                record.append("\n");
                DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
                outToClient.writeBytes(line + "\n");
            }
            producer.send(new ProducerRecord<String, String>(TOPIC,record.toString()));
            System.out.println(record.toString());
            record = new StringBuilder();
        }
    }
}