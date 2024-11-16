package it.polimi.middleware.kafka.exercise1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class C1 {
    final static private String topicA = "topicA";
    final static private String serverAdr = "localhost:9092";
    final static private String groupID = "hello";


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAdr);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);

        KafkaConsumer<Integer, String> c1 = new KafkaConsumer<>(props);

        c1.subscribe(Collections.singletonList(topicA));

        while(true) {
            final ConsumerRecords<Integer, String> records = c1.poll(Duration.of(20, ChronoUnit.SECONDS));
            for( final ConsumerRecord<Integer, String> record: records){
                System.out.println(record.value());
            }

        }



    }
}
