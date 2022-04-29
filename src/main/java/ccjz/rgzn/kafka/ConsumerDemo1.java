package ccjz.rgzn.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;



public class ConsumerDemo1 {

    private static final String SERVERS = "node1:9092,node2:9092,node3:9092";

    public static void main(String[] args) throws InterruptedException {

        // Atomic 原子性的，线程安全的
        AtomicBoolean isRunning = new AtomicBoolean(true);

        // 参数配置
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);
        // 自动设置读取的起始offset，值可以是： earliest， latest ，none
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // 自动提交消费位移offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        // 设置该消费者所属的组id
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"g1");


        Thread thread = new Thread(new ConsurmerTask(props,isRunning));

        thread.start();

        //主线程继续做别的业务
        Thread.sleep(60000);

        isRunning.set(false);

    }

}
