package keyreduce;

import bean.PhoneDetail;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lihaoran
 */
public class AggTest {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.readTextFile("/Users/lihaoran/Documents/flink-projects/flink-test/src/main/resources/phone.txt");
        //使用匿名类
        SingleOutputStreamOperator<PhoneDetail> map1 = dataStreamSource.map(new MapFunction<String, PhoneDetail>() {
            @Override
            public PhoneDetail map(String s) throws Exception {
                String[] fields = s.split(",");
                return new PhoneDetail(fields[0], new Double(fields[1]));
            }
        });
        //使用lambda表达式，返回有泛型时会有泛型擦除
        SingleOutputStreamOperator<PhoneDetail> mapStream = dataStreamSource.map(line -> {
            String[] fields = line.split(",");
            return new PhoneDetail(fields[0], new Double(fields[1]));
        });
        /**
         * 聚合之前必须先分组，分组是逻辑操作，不是物理操作
         * keyBy通过位置确定元素只能用于元祖，bean类不适用
         */
        KeyedStream<PhoneDetail, Tuple> keyedStream = mapStream.keyBy("phoneName");
        KeyedStream<PhoneDetail, String> phoneDetailStringKeyedStream = mapStream.keyBy(PhoneDetail::getPhoneName);
        /**
         * 滚动聚合
         * 带By和不带By的区别，以max为例
         * 不带By显示的那个字段为最大值，其他字段数据都不改变的那一行
         * 带By显示的是那个字段数据最大值对应的那一行
         */
        SingleOutputStreamOperator<PhoneDetail> priceStream = phoneDetailStringKeyedStream.maxBy("price");
//        priceStream.print();
        /**
         * reduce聚合
         */
        SingleOutputStreamOperator<PhoneDetail> reduce = keyedStream.reduce(new ReduceFunction<PhoneDetail>() {
            /**
             *
             * @param phoneDetail 之前的聚合结果
             * @param t1 当前最新的数据
             * @return
             * @throws Exception
             */
            @Override
            public PhoneDetail reduce(PhoneDetail phoneDetail, PhoneDetail t1) throws Exception {
                return new PhoneDetail(phoneDetail.getPhoneName(), Math.max(phoneDetail.getPrice(), t1.getPrice()));
            }
        });
        reduce.print();
//        SingleOutputStreamOperator<PhoneDetail> reduce1 = keyedStream.reduce((curState, newData) -> {
//            return new PhoneDetail(curState.getPhoneName(), Math.max(curState.getPrice(), newData.getPrice()));
//        });
        env.execute();
    }
}
