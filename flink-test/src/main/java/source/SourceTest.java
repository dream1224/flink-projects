package source;

import bean.PhoneDetail;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author lihaoran
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取集合
        DataStreamSource<PhoneDetail> phoneDetailDataStreamSource = env.fromCollection(Arrays.asList(
                new PhoneDetail("mi", 4000.0),
                new PhoneDetail("huawei", 5000.0),
                new PhoneDetail("apple", 6000.0)
        ));

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4);

        //读取文件
        DataStreamSource<String> fileSource = env.readTextFile("/Users/lihaoran/Documents/flink-projects/flink-test/src/main/resources/phone.txt");

        //读取kafka
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "39.103.182.237:9092");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>("topic", new SimpleStringSchema(), properties);
        fileSource.addSink(kafkaProducer);
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "groupid");
//        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), properties));
//        DataStreamSource<String> kafkaConsumer = env.addSource(new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), properties));
//        //自定义数据源
//        DataStream<PhoneDetail> dataStream = env.addSource(new CustomizeSource());
//        dataStream.print();
//        env.execute();
//    }
//
//    private static class CustomizeSource implements org.apache.flink.streaming.api.functions.source.SourceFunction<PhoneDetail> {
//        //定义标志位控制数据生成
//        private Boolean runState = true;
//        @Override
//        public void run(SourceContext<PhoneDetail> ctx) throws Exception {
//            Random random = new Random();
//            HashMap<String, Double> hashMap = new HashMap<>();
//            for (int i = 8; i < 12; i++) {
//                hashMap.put("sensor" + i, abs(random.nextGaussian()*10000));
//            }
//            while (runState){
//                for (String brand : hashMap.keySet()) {
//                    Double newPrice = hashMap.get(brand) + random.nextGaussian()*100;
//                    hashMap.put(brand,newPrice);
//                    ctx.collect(new PhoneDetail(brand,newPrice));
//                }
//                //控制输出频率
//                Thread.sleep(5000);
//            }
//        }
//        @Override
//        public void cancel() {
//            runState = false;
//        }
    }
}
