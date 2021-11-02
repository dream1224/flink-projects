package common.conn;

import common.mapping.Constants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author lihaoran
 */
public class KafkaUtils {
    // 必须声明为静态，因为方法是静态的
    private static Properties properties = new Properties();
    // 定义默认主题，匹配不上的数据都发到这个topic
    private static String defaultTopic = Constants.DEFAULT_DWD_TOPIC;

    // 用静态代码块
    static {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);

    }

    /**
     * 根据schema创建kafka生产者
     *
     * @param <T>
     * @return
     */
    public static <T> FlinkKafkaProducer<T> makeKafkaProducerBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        return new FlinkKafkaProducer<T>(defaultTopic, kafkaSerializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * 获取生产者对象
     *
     * @param topic 主题
     * @return
     */
    public static FlinkKafkaProducer<String> makeKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    /**
     * 获取消费者对象
     *
     * @param topic   主题
     * @param groupId 消费者组
     * @return
     */
    public static FlinkKafkaConsumer<String> makeKafkaConsumer(String topic, String groupId) {
        // 添加消费者组
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

}
