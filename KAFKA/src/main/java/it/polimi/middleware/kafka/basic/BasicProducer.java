package it.polimi.middleware.kafka.basic;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class BasicProducer {
    private static final String defaultTopic = "topicA";

    private static final int numMessages = 100000;
    private static final int waitBetweenMsgs = 50;
    private static final boolean waitAck = false;

    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) {


        // If there are no arguments, publish to the default topic
        // Otherwise publish on the topics provided as argument
        List<String> topics = args.length < 1 ?
                Collections.singletonList(defaultTopic) :
                Arrays.asList(args);

        final Properties props = new Properties();
        // Set the addresses of remote Kafka brokers, they can be many (in this demo only the local one).
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        // The key and value serializers are used to encode the key and value objects to byte arrays
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Entry point for a KafkaProducer is the KafkaProducer class, generic over the key and value types used to encode the messages
        // The constructor wants a property object with the configurations
        final KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        final Random r = new Random();

        for (int i = 0; i < numMessages; i++) {
            final String topic = topics.get(r.nextInt(topics.size()));  // Randomly select a topic (a topic is automatically created if it does not exist)
            final Integer key = r.nextInt(10000);
            final String value = "Val" + i;
            System.out.println(
                    "Topic: " + topic +
                    "\tKey: " + key +
                    "\tValue: " + value
            );

            // The ProducerRecord class is used to encapsulate the key, value, and topic and then send them to the broker
            final ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, key, value);
            // The Future structure holds a RecordMetadata that contains metadata about the message sent 
            final Future<RecordMetadata> future = producer.send(record);

            if (waitAck) {
                try {
                    RecordMetadata ack = future.get();
                    System.out.println("Ack for topic " + ack.topic() + ", partition " + ack.partition() + ", offset " + ack.offset());
                } catch (InterruptedException | ExecutionException e1) {
                    e1.printStackTrace();
                }
            }

            try {
                Thread.sleep(waitBetweenMsgs);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}