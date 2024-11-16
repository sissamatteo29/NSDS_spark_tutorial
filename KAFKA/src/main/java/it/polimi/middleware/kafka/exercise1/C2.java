package it.polimi.middleware.kafka.exercise1;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class C2 {

    final static private String consumerTopic = "topicA";
    final static private String producerTopic = "topicB";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        KafkaConsumer<Integer, String> c2 = new KafkaConsumer<>(props);

        ArrayList<String> suscriptionTopics = new ArrayList<>();
        suscriptionTopics.add(consumerTopic);

        c2.subscribe(suscriptionTopics);

        Properties propsproducer = new Properties();
        propsproducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsproducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        propsproducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> p2 = new KafkaProducer<>(propsproducer);

        while (true) { 
            ConsumerRecords<Integer, String> records = c2.poll(Duration.of(5, ChronoUnit.SECONDS));

            for (ConsumerRecord<Integer, String> record : records) {
                String extractedvalue = record.value().toLowerCase();
                Integer extractedkey = record.key();
                ProducerRecord<Integer, String> prec2 = new ProducerRecord<Integer,String>(producerTopic, extractedkey, extractedvalue);
                p2.send(prec2);
                System.out.println("Sent Value: "+extractedvalue+" with key: "+extractedkey);
            }
        }


    }
}
