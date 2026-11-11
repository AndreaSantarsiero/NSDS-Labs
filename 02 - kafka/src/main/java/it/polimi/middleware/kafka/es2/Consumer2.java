package it.polimi.middleware.kafka.es2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;


public class Consumer2 {
    private static final String defaultGroupId = "groupB";
    private static final String consumerTopic = "topicA";
    private static final String producerTopic = "topicB";

    private static final String serverAddr = "localhost:9092";
    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 15000;

    // Default is "latest": try "earliest" instead
    private static final String offsetResetStrategy = "latest";

    public static void main(String[] args) throws Exception {
        // If there are arguments, use the first as group and the second as topic.
        // Otherwise, use default group and topic.
        String groupId = args.length >= 1 ? args[0] : defaultGroupId;
        String topic = args.length >= 2 ? args[1] : consumerTopic;

        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));

        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);

        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
            for (final ConsumerRecord<String, String> record : records) {
                System.out.println("processing a new message with value " +  record.value());
                Thread.sleep(650);
                final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(producerTopic, record.key(), record.value().toLowerCase());
                final Future<RecordMetadata> future = producer.send(producerRecord);
            }
        }
    }
}