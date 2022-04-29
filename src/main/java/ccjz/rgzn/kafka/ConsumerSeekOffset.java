package ccjz.rgzn.kafka;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

public class ConsumerSeekOffset {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node1:9092,node2:9092,node3:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"g1");
        //创建kafka实例对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        //订阅主题
        kafkaConsumer.subscribe(Arrays.asList("tpc_1"));

        //先拉取一次消息
        kafkaConsumer.poll(Duration.ofMillis(10000));
        //先看看被分配了哪些topic中的分区
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        //对于被分配的分区，全部统一定位到offset=100的位置成为初始偏移量
        for (TopicPartition topicPartition : assignment) {
            kafkaConsumer.seek(topicPartition,100);
        }
        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                // 做一些业务处理
                System.out.println(record.key() + ","
                        + record.value() +","
                        +record.topic() + ","
                        +record.partition()+ ","
                        +record.offset());
                System.out.println("----------------------分割线----------------------------");
            }
        }
    }






}
