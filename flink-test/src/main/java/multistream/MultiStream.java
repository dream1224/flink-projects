package multistream;

import bean.PhoneDetail;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class MultiStream {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("/Users/lihaoran/Documents/flink-projects/flink-test/src/main/resources/phone.txt");

        DataStream<PhoneDetail> streamOperator = dataStreamSource.map(new MapFunction<String, PhoneDetail>() {
            @Override
            public PhoneDetail map(String s) throws Exception {
                String[] split = s.split(",");
                return new PhoneDetail(split[0], new Double(split[1]));
            }
        });
        streamOperator.print();
        // 随机分区
        streamOperator.shuffle();
        // 平衡分区 Roundrobin循环
        streamOperator.rebalance();
        // 按比例分区
        streamOperator.rescale();
        // 广播分区 适合小数据集
        streamOperator.broadcast();
        // 自定义分区
//        streamOperator.partitionCustom(new Partitioner<String>() {
//            Random random = new Random();
//            @Override
//            public int partition(String o, int i) {
////                return o.hashCode()/i;
//                if(o.contains("flink")) {
//                    return 0;
//                }else{
//                    return random.nextInt(i);
//                }
//            }
//        },1);
//        // 数据汇总到一个分区
//        streamOperator.global();
//        System.out.println(Long.MIN_VALUE);

        // 分流 split已过时，使用side output替代
//        streamOperator
        //connect只能两条流，但是流的类型可不同，union可以多条流。流的类型必须相同
        env.execute();
    }
}
