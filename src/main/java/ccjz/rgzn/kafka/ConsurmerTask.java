package ccjz.rgzn.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsurmerTask implements Runnable{

    Properties props = null;
    AtomicBoolean isRunning = null;

    public ConsurmerTask(Properties props, AtomicBoolean isRunning) {
        this.props = props;
        this.isRunning = isRunning;
    }

    @Override
    public void run() {
        //构造consumer实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //订阅主题
        consumer.subscribe(Arrays.asList("tpc_1"));

        //拉取消息
        while (isRunning.get()){

            //每一次poll得到的结果中，可能包含多个topic的多个partition的消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));

            for (ConsumerRecord<String, String> record : records) {
                //do some process做一些处理
                System.out.println(record.key()+","
                        +record.value()+","
                        +record.topic()+","
                        +record.partition()+","
                        +record.offset());
                System.out.println("------------------------分割线---------------------------");

            }
        }

        consumer.close(Duration.ofMillis(1000));

    }

}
