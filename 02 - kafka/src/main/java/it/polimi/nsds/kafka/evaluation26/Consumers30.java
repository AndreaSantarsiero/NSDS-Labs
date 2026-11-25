// package it.polimi.nsds.kafka.eval;
package it.polimi.nsds.kafka.evaluation26;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;


// Group number: 30
// Group members: Andrea Santarsiero, Florian Erwan Quentin Mouro, Marco Sapienza

// 1) How many partitions can the topics "temperature" and "humidity" have?
// Both topics can have as many partitions as wished.

// 2) Is there any relation between the number of partitions in "temperature" and "humidity"?
// "temperature" and "humidity" must have the same number of partitions because the Monitor needs to
// process both events from the same key. This guarantees that the same key will be associated to
// the same partition in both topics, and thus be processed on the same instance of the Monitor.

// 3) Is it possible to have more than one instance of Monitor? If so, what is the relation between their group id?
// Yes, it is possible to have more instances of Monitor. If they all share the same groupId, there will be no
// additional duplication of events on the "alerts" topic. If the number of partitions of "temperatures" (which must be
// the same of the number of partitions of "humidities") is smaller than the number of instances with the same groupId,
// then the remaining instances will stay IDLE. If Monitor instances have different groupIds (suppose n), since we're
// assuming that it never crashed, exactly n events will be generated on the "alerts" topic.

// 4) How many partitions can the topic "alerts" have?
// As much as wished since it is guaranteed that every key will be associated to the same partition over time.

// 5) Is it possible to have more than one instance of Reactor? If so, what is the relation between their group id?
// Yes, it is. If multiple instances of the Reactor are created with the same groupId, nothing would change except
// for the fact that events assigned to different partitions could be processed by different Reactor instances.
// If multiple instances of the Reactor are created with different groupId (suppose n), the same "processing" and
// "processedAlerts" events will be generated at least n times.

// 6) How many partitions can the topic "processedAlerts" have?
// As much as wished. Monitor and Reactor are not subscribed to the "processedAlerts" topic

// 7) Which additional topics, if any, do you use? How many partitions can they have?
// We're using the additional "processing" topic to avoid reprocessing Reaction1 in the case where the Reactor crashes
// while computing the Reaction2. It can have as many partitions as wished, and the number of partitions is not
// correlated to the number of partitions of the "alerts" topic since there's no shared state between Reaction1
// and Reaction2.



public class Consumers30 {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];
        switch (stage) {
            case 1:
                new Monitor(serverAddr, groupId).execute();
                break;
            case 2:
                new Reactor(serverAddr, groupId).execute();
                break;
            default:
                System.err.println("Wrong stage");
        }
    }



    // Simulates the first reaction of the system to an alert (provided, do not modify)
    private static void reaction1(String room, String alert) {
        System.out.println("[reaction1] room=" + room + " " + alert);
    }

    // Simulates the second reaction of the system to an alert (provided, do not modify)
    private static void reaction2(String room, String alert) {
        System.out.println("[reaction2] room=" + room + " " + alert);
    }




    private static class Monitor {
        private final String serverAddr;
        private final String consumerGroupId;
        private static final boolean autoCommit = true;
        private static final int autoCommitIntervalMs = 15000;
        private static final String offsetResetStrategy = "latest";

        private static final String temperaturesTopic = "temperature";
        private static final String humiditiesTopic = "humidity";
        private static final String outputTopic = "alerts";

        private static final int windowSize = 3;
        private static final double temperatureThreshold = 28;
        private static final double humidityThreshold = 40;

        private final Map<String, List<Integer>> temperatures ;
        private final Map<String, List<Integer>> humidities;


        public Monitor(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
            this.temperatures = new HashMap<>();
            this.humidities = new HashMap<>();
        }


        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
            consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(List.of(temperaturesTopic, humiditiesTopic));


            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> record : records) {
                    System.out.println("Partition: " + record.partition() +
                            "\tOffset: " + record.offset() +
                            "\tKey: " + record.key() +
                            "\tValue: " + record.value()
                    );

                    String key = record.key();
                    temperatures.putIfAbsent(key, new ArrayList<>());
                    humidities.putIfAbsent(key, new ArrayList<>());

                    if(record.topic().equals(temperaturesTopic)) {
                        temperatures.get(key).add(record.value());
                    }
                    else if(record.topic().equals(humiditiesTopic)) {
                        humidities.get(key).add(record.value());
                    }
                    else{
                        System.out.println("ERROR: unsupported topic: " +  record.topic());
                        continue;
                    }

                    List<Integer> temperaturesByKey = temperatures.get(key);
                    List<Integer> humiditiesByKey = humidities.get(key);

                    if(temperaturesByKey.size() >= windowSize && humiditiesByKey.size() >= windowSize) {
                        double avgTemperature = 0,  avgHumidity = 0;

                        for(int i = 1; i <= windowSize; i++) {
                            avgTemperature += temperaturesByKey.get(temperaturesByKey.size() - i);
                            avgHumidity += humiditiesByKey.get(humiditiesByKey.size() - i);
                        }

                        avgTemperature /= windowSize;
                        avgHumidity /= windowSize;

                        if(avgTemperature > temperatureThreshold && avgHumidity < humidityThreshold) {
                            producer.send(new ProducerRecord<>(outputTopic, key, "Alert!"));
                            System.out.println("[ATTENTION] Sending Alert!");
                        }
                    }
                }
            }
        }
    }




    private static class Reactor {

        private final String serverAddr;
        private final String consumerGroupId;
        private static final boolean autoCommit = false;
        private static final String offsetResetStrategy = "earliest";
        private static final String inputTopic = "alerts";
        private static final String processingTopic = "processing";
        private static final String outputTopic = "processedAlerts";


        public Reactor(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }


        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(List.of(inputTopic,  processingTopic));


            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, String> record : records) {
                    System.out.println("Partition: " + record.partition() +
                            "\tOffset: " + record.offset() +
                            "\tKey: " + record.key() +
                            "\tValue: " + record.value()
                    );

                    if(record.topic().equals(inputTopic)) {
                        reaction1(record.key(), record.value());
                        producer.send(new ProducerRecord<>(processingTopic, record.key(), record.value()));
                        // sync this message with the broker before processing the next record
                        final Map<TopicPartition, OffsetAndMetadata> processedRecord = new HashMap<>();
                        processedRecord.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        );
                        producer.flush();
                        consumer.commitSync(processedRecord);
                    }
                    else if(record.topic().equals(processingTopic)) {
                        reaction2(record.key(), record.value());
                        producer.send(new ProducerRecord<>(outputTopic, record.key(), record.value()));
                        // sync this message with the broker before processing the next record
                        final Map<TopicPartition, OffsetAndMetadata> processedRecord = new HashMap<>();
                        processedRecord.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        );
                        producer.flush();
                        consumer.commitSync(processedRecord);
                    }
                    else{
                        System.out.println("ERROR: unsupported topic: " +  record.topic());
                        continue;
                    }
                }
            }
        }
    }
}