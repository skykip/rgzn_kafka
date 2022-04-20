package ccjz.rgzn.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
/**
 * kafka生产者开发示例
 * 2022.4.1 by yzl
 */

public class ProducerDemo {
    private static final String SERVERS = "node1:9092,node2:9092,node3:9092";


    public static <string> void main(String[] args) throws IOException {
        /**
         * 步骤1.配置生产者的参数
         * @param args
         */
        Properties props = new Properties();
        //配置方式1-->比较容易会将参数的名称写错
//        props.load(ProducerDemo.class.getClassLoader().getResourceAsStream("client.properties"));
//        props.put("bootstarp.servers","node1:9092,node2:9092,node3:9092");

        //配置方式2，利用常量类，去进行配置，不容易写错参数名，比较容易记忆
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.RETRIES_CONFIG,3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,10);
//        props.put(ProducerConfig.LINGER_MS_CONFIG,10000);
//        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,10);
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,1024);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //关闭kafka幂等性的功能
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"false");

        /**
         * 步骤2.创建相应的生产者实例
         * @param args
         */
        KafkaProducer<String,String> producer =  new KafkaProducer<>(props);

        /**
         * 步骤3.构建待发送的消息
         * @param args
         */
        for(int i = 0;i<100;i++) {
            ProducerRecord<String, String> msg = new ProducerRecord<>("tpc_1", "name"+i, "bigdata19-yzl+" +RandomStringUtils.randomAlphabetic(4,7) );
            /**
             * 步骤4.发送消息
             * @param args
             */
            producer.send(msg);
        }

        /**
         * 步骤5.关闭生产者实例
         * @param args
         */
        producer.close();








    }

}
