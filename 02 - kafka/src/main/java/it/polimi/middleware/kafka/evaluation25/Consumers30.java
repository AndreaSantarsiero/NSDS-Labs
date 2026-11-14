package it.polimi.middleware.kafka.evaluation25;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;


// Group number: 30
// Group members: Andrea Santarsiero, Florian Erwan Quentin Mouro, Marco Sapienza

// 1) Is it possible to have more than one partition for topics "sensors1" and "sensors2"? Is there any relation between the number of their partitions?
// Yes it is, but since the Merger needs to process the same keys for both topics, they must have the same number of partitions. This guarantees that the same key will be
// associated to the same partition in both topics, and thus be stored on the same instance of the Merger.

// 2) Is it possible to have more than one instance of Merger? If so, what is the relation between their group id?
// If multiple instances of the Merger are created with the same groupId, nothing would change except for the replication of partitions over the Merger instances. If multiple
// instances of the Merger are created with different groupId (suppose n), the same "merged" event would be generated at least n times, and I suppose that this is not a wanted behavior

// 3) Is it possible to have more than one partition for topic "merged"?
// Yes it is, since it is guaranteed that every key will be associated to the same partition over time and since the Validator is subscribed to the "merged" topic

// 4) Is it possible to have more than one instance of Validator? If so, what is the relation between their group id?
// If multiple instances of the Validator are created with the same groupId, nothing would change except for the replication of partitions over the Validator instances. If multiple
// instances of the Validator are created with different groupId (suppose n), the same "output" events would be generated exactly n times. In general, every instance of a
// Validator must have a unique transactionalId in order to avoid conflicts at runtime



public class Consumers30 {

    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];
        switch (stage) {
            case 1:
                new Merger(serverAddr, groupId).execute();
                break;
            case 2:
                String producerTransactionalId = args[2];
                new Validator(serverAddr, groupId, producerTransactionalId).execute();
                break;
            default:
                System.err.println("Wrong stage");
        }
    }




    // Merges the events on the topics "sensors1" and "sensors2" by performing a sum operation on values with the same key
    // Guarantees at least once evaluation of every input message
    private static class Merger {

        private final String serverAddr;
        private final String consumerGroupId;
        private static final boolean autoCommit = false;
        private static final String offsetResetStrategy = "earliest";
        private static final List<String> consumerTopics = List.of("sensors1", "sensors2");
        private static final String producerTopic = "merged";

        private final Map<String, Integer> s1Samples;
        private final Map<String, Integer> s2Samples;


        public Merger(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
            this.s1Samples = new HashMap<>();
            this.s2Samples = new HashMap<>();
        }


        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(consumerTopics);


            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofSeconds(1));
                for (final ConsumerRecord<String, Integer> record : records) {
                    System.out.println("Partition: " + record.partition() +
                            "\tOffset: " + record.offset() +
                            "\tKey: " + record.key() +
                            "\tValue: " + record.value()
                    );
                    int sum = 0;

                    if(record.topic().equals("sensors1")){
                        s1Samples.put(record.key(), record.value());
                        sum = record.value() + s2Samples.getOrDefault(record.key(), 0);
                    }
                    else if(record.topic().equals("sensors2")) {
                        s2Samples.put(record.key(), record.value());
                        sum = record.value() + s1Samples.getOrDefault(record.key(), 0);
                    }
                    else{
                        continue;
                    }

                    producer.send(new ProducerRecord<>(producerTopic, record.key(), sum));
                }

                if (!records.isEmpty()) {
                    producer.flush();
                    consumer.commitSync();
                }
            }
        }
    }




    // Forwards the "merged" events on both "output1" and "output2" topics
    // Guarantees atomicity of every forward in both topics
    private static class Validator {
        private final String serverAddr;
        private final String consumerGroupId;
        private final String producerTransactionalId;
        private static final boolean autoCommit = false;
        private static final String offsetResetStrategy = "latest";
        private static final String consumerTopic = "merged";
        private static final List<String> producerTopics = List.of("output1", "output2");


        public Validator(String serverAddr, String consumerGroupId, String producerTransactionalId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
            this.producerTransactionalId = producerTransactionalId;
        }


        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(consumerTopic));


            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
            producer.initTransactions();

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMinutes(5));

                if(records.isEmpty()){
                    continue;
                }
                producer.beginTransaction();

                for (final ConsumerRecord<String, Integer> record : records) {
                    System.out.println("Partition: " + record.partition() +
                            "\tOffset: " + record.offset() +
                            "\tKey: " + record.key() +
                            "\tValue: " + record.value()
                    );
                    for(String topic : producerTopics){
                        producer.send(new ProducerRecord<>(topic, record.key(), record.value()));
                    }
                }

                final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                for (final TopicPartition partition : records.partitions()) {
                    final List<ConsumerRecord<String, Integer>> partitionRecords = records.records(partition);
                    final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                }

                producer.sendOffsetsToTransaction(map, consumer.groupMetadata());
                producer.commitTransaction();
            }
        }
    }
}