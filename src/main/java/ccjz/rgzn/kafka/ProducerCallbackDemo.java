package ccjz.rgzn.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerCallbackDemo {
    private static final String SERVERS = "node1:9092,node2:9092,node3:9092";

    public static void main(String[] args) {
        /**
         * 步骤1.配置生产者的参数
         * @param args
         */
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /**
         * 步骤2.创建相应的生产者实例
         * @param args
         */
        KafkaProducer<String,String> producer =  new KafkaProducer<>(props);

        /**
         * 步骤3.构建待发送的消息
         * @param args
         */
        for(int i=0;i<10;i++) {
            ProducerRecord<String,String> rcd = new ProducerRecord<String,String>("tpc_6","key"+i,"value"+i);

            /**
             * 步骤4.发送消息
             * @param args
             */
            producer.send(rcd, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //如果响应是成功的，则recordMetadata是有值的
                    if (recordMetadata !=null){
                        System.out.println(recordMetadata.topic());
                        System.out.println(recordMetadata.offset());
                        System.out.println(recordMetadata.serializedKeySize());
                        System.out.println(recordMetadata.serializedValueSize());
                        System.out.println(recordMetadata.timestamp());
                    }
                }
            });
        }

        /**
         * 步骤5.关闭生产者实例
         * @param args
         */
        producer.close();
    }
}
