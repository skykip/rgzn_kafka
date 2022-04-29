package ccjz.rgzn.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminDemo {
    private static final String SERVERS = "node1:9092,node2:9092,node3:9092";
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);

        //1.构造一个管理客户端的对象
        AdminClient adminClient = KafkaAdminClient.create(props);

        //2.列出集群中的主题信息
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        KafkaFuture<Set<String>> names = listTopicsResult.names();
        Set<String> topicNames = names.get();
        System.out.println(topicNames);


        //3.查看一个topic的具体信息
        DescribeTopicsResult tpc_1 = adminClient.describeTopics(Arrays.asList("tpc_1","tpc_2","tpc_10"));
        KafkaFuture<Map<String, TopicDescription>> future = tpc_1.all();
        Map<String, TopicDescription> stringTopicDescriptionMap = future.get();//get()会阻塞直到拿到返回值
        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
        for (Map.Entry<String, TopicDescription> entry : entries) {
            System.out.println(entry.getKey());
            TopicDescription desc = entry.getValue();
            System.out.println(desc.name()+","+desc.partitions());
            System.out.println("------------分割线---------------");
        }


        //4.创建topic
//        HashMap<Integer, List<Integer>> partitions = new HashMap<>();
//        partitions.put(0,Arrays.asList(0,2));
//        partitions.put(1,Arrays.asList(1,2));
//        partitions.put(2,Arrays.asList(0,1));
//
//        NewTopic tpc_10 = new NewTopic("tpc_10",partitions);

//        NewTopic tpc_11 = new NewTopic("tpc_10",2,(short) 3);

//        adminClient.createTopics(Arrays.asList(tpc_10));



        adminClient.close();


    }


}
