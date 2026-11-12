package it.polimi.middleware.kafka.admin;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

public class TopicManager {
    private static final String defaultTopicName = "topicA";
    private static final int defaultTopicPartitions = 2;
    private static final short defaultReplicationFactor = 1;

    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) throws Exception {
        final String topicName = args.length >= 1 ? args[0] : defaultTopicName;
        final int topicPartitions = args.length >= 2 ? Integer.parseInt(args[1]) : defaultTopicPartitions;
        final short replicationFactor = args.length >= 3 ? Short.parseShort(args[2]) : defaultReplicationFactor;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        AdminClient adminClient = AdminClient.create(props);

        ListTopicsResult listResult = adminClient.listTopics();
        Set<String> topicsNames = listResult.names().get();
        System.out.println("Available topics: " + topicsNames);

        if (topicsNames.contains(topicName)) {
            System.out.println("Deleting topic " + topicName);
            DeleteTopicsResult delResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            delResult.all().get();
            // Wait for the cluster real deletion since it can takes seconds
            while (adminClient.listTopics().names().get().contains(topicName)) {
                Thread.sleep(1000);
            }
            System.out.println("Done!");

        }

        System.out.println("Adding topic " + topicName + " with " + topicPartitions + " partitions");
        NewTopic newTopic = new NewTopic(topicName, topicPartitions, replicationFactor);
        CreateTopicsResult createResult = adminClient.createTopics(Collections.singletonList(newTopic));
        createResult.all().get();
        // Wait for the cluster real addition since it can takes seconds
        while (!adminClient.listTopics().names().get().contains(topicName)) {
            Thread.sleep(1000);
        }
        System.out.println("Done!");
        adminClient.close();
    }
}
