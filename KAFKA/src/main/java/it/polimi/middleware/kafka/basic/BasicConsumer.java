package it.polimi.middleware.kafka.basic;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BasicConsumer {
    private static final String defaultGroupId = "groupA";
    private static final String defaultTopic = "topicA";

    private static final String serverAddr = "localhost:9092";
    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 15000;

    // Default is "latest": try "earliest" instead
    private static final String offsetResetStrategy = "latest";

    public static void main(String[] args) {
        // If there are arguments, use the first as group and the second as topic.
        // Otherwise, use default group and topic.
        String groupId = args.length >= 1 ? args[0] : defaultGroupId;
        String topic = args.length >= 2 ? args[1] : defaultTopic;

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr); // Set the addresses of remote Kafka brokers
        // Unique identifier of the consumer group to which this consumer belongs
        // If two consumers have the same group ID, and they subscribe to the same topic, each message will be delivered to one of them (not both)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // The auto commit feature allows the consumer to commit the offset of the last message received on the kafka brokers (holding the offsets for consumers as events)
        // With the default configuration, the consumer commits the offset every 5 seconds (not every time a new message is received)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));

        // The offset reset strategy is used when the consumer starts reading from a partition for the first time 
        // earliest = receive all messages from beginning
        // latest = receive only new messages 
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);

        // The key and value deserializers are used to decode the byte arrays into key and value objects
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // The KafkaConsumer class is the entry point for a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // Subscribe to the same topic of the publisher
        consumer.subscribe(Collections.singletonList(topic));
        
        while (true) {

            // Blocking method call that waits for records from the broker (a timeout can be set)
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));

            // Iterate over the records received and print the internal metadata
            for (final ConsumerRecord<String, String> record : records) {
                System.out.print("Consumer group: " + groupId + "\t");
                System.out.println("Partition: " + record.partition() +
                        "\tOffset: " + record.offset() +
                        "\tKey: " + record.key() +
                        "\tValue: " + record.value()
                );
            }
        }
    }
}