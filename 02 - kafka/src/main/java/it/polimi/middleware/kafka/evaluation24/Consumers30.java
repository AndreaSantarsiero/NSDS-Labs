package it.polimi.middleware.kafka.evaluation24;

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
import java.time.temporal.ChronoUnit;
import java.util.*;


// Group number: 30
// Group members: Andrea Santarsiero, Florian Erwan Quentin Mouro, Marco Sapienza

// 1) Number of partitions for inputTopic (min, max): (1, N) any positive integer
// 2) Number of partitions for outputTopic1 (min, max): (1, N) any positive integer. Since the key is always "sum", only one will be used
// 3) Number of partitions for outputTopic2 (min, max): (1, N) any positive integer

// 4) Number of instances of Consumer1 (and groupId of each instance) (min, max): (1, 1)
// Since the tumbling window is global, more instances with the same groupId would split the partitions and thus obtain wrong sums. In addition,
// multiple instances with different groupId would duplicate the "outputTopic1" events, breaking exactly once guarantees

// 5) Number of instances of Consumer2 (and groupId of each instance) (min, max): (1, #partitions of inputTopic)
// All instances share the same groupId. Additional instances stay idle.

// 6) Please, specify below any relation between the number of partitions for the topics and the number of instances of each Consumer
// The number of Consumer2 instances is bounded by the number of partitions of "inputTopic" events. Key-based partitioning guarantees that all records
// with the same key are processed by the same instance. Consumer1 does not depend on the number of partitions, since a single instance must read all of them.
// Consumer1 and Consumer2 must use different groupIds since they both need to process all "inputTopic" events, otherwise they would split inputTopic partitions.



public class Consumers30 {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int consumerId = Integer.valueOf(args[0]);
        String groupId = args[1];
        if (consumerId == 1) {
            String producerTransactionalId = args[2];
            Consumer1 consumer = new Consumer1(serverAddr, groupId, producerTransactionalId);
            consumer.execute();
        } else if (consumerId == 2) {
            Consumer2 consumer = new Consumer2(serverAddr, groupId);
            consumer.execute();
        }
    }




    private static class Consumer1 {
        private final String serverAddr;
        private final String consumerGroupId;
        private final String producerTransactionalId;
        private static final boolean autoCommit = false;
        private static final String inputTopic = "inputTopic";
        private static final String outputTopic = "outputTopic1";
        private static final String outputKey = "sum";

        private static final int tumblingWindowSize = 10;
        private final List<Integer> tumblingWindow;


        public Consumer1(String serverAddr, String consumerGroupId, String producerTransactionalId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
            this.producerTransactionalId = producerTransactionalId;
            this.tumblingWindow = new ArrayList<>();
        }


        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));


            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
            producer.initTransactions();

            final Map<TopicPartition, OffsetAndMetadata> tumblingWindowRecords = new HashMap<>();

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> record : records) {
                    System.out.println("Partition: " + record.partition() +
                            "\tOffset: " + record.offset() +
                            "\tKey: " + record.key() +
                            "\tValue: " + record.value()
                    );
                    tumblingWindow.add(record.value());
                    tumblingWindowRecords.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));

                    if(tumblingWindow.size() >= tumblingWindowSize){
                        int sum = 0;

                        for(int value : tumblingWindow){
                            sum += value;
                        }

                        producer.beginTransaction();
                        producer.send(new ProducerRecord<>(outputTopic, outputKey, sum));

                        producer.sendOffsetsToTransaction(tumblingWindowRecords, consumer.groupMetadata());
                        producer.commitTransaction();
                        tumblingWindow.clear();
                        tumblingWindowRecords.clear();
                    }
                }
            }
        }
    }




    private static class Consumer2 {
        private final String serverAddr;
        private final String consumerGroupId;
        private static final boolean autoCommit = true;
        private static final int autoCommitIntervalMs = 15000;
        private static final String offsetResetStrategy = "latest";
        private static final String inputTopic = "inputTopic";
        private static final String outputTopic = "outputTopic2";

        private static final int tumblingWindowSize = 10;
        private final Map<String, List<Integer>> tumblingWindowByKey;


        public Consumer2(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
            this.tumblingWindowByKey = new HashMap<>();
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

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));


            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> record : records) {
                    System.out.println("Partition: " + record.partition() +
                            "\tOffset: " + record.offset() +
                            "\tKey: " + record.key() +
                            "\tValue: " + record.value()
                    );

                    if(!tumblingWindowByKey.containsKey(record.key())){
                        tumblingWindowByKey.put(record.key(), new ArrayList<>());
                    }

                    List<Integer> tumblingWindow = tumblingWindowByKey.get(record.key());
                    tumblingWindow.add(record.value());

                    if(tumblingWindow.size() >= tumblingWindowSize){
                        int sum = 0;

                        for(int value : tumblingWindow){
                            sum += value;
                        }

                        producer.send(new ProducerRecord<>(outputTopic, record.key(), sum));
                        tumblingWindow.clear();
                    }
                }
            }
        }
    }
}