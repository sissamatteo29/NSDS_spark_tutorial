
package it.polimi.codebase;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;

public class AdminI {

    private AdminClient client;
    
    public AdminI(String addr) {
        Properties adminProp = new Properties();
        adminProp.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, addr);
        client = AdminClient.create(adminProp);

    }


    public int numberOfPartitions(String topic) {
        DescribeTopicsResult totalDescription = client.describeTopics(Collections.singletonList(topic));
        try {
            TopicDescription topicDescription = totalDescription.values().get(topic).get();
            return topicDescription.partitions().size();
        } catch (Exception e) {
        }
        return 0;
    }

    public int addPartitions(String topic, int totalPartitions) {

        client.createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(totalPartitions)));

        return numberOfPartitions(topic);
    }



    
}