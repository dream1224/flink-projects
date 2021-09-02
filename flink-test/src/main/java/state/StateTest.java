package state;

import bean.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


import java.util.Collections;
import java.util.List;

/**
 * @author lihaoran
 */
public class StateTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置状态后端 checkpoint和本地状态的的存储
        env.setStateBackend(new MemoryStateBackend(true));
        env.setStateBackend(new FsStateBackend(""));

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<User> userStream = dataStream.map(new MapFunction<String, User>() {
            @Override
            public User map(String value) throws Exception {
                String[] split = value.split(",");
                return new User(split[0], new Double(split[1]), new Long(split[2]));
            }
        });
        // 统计当前分区数据个数，算子状态跟key无关，一个分区保存一个状态
//        SingleOutputStreamOperator<Integer> map = userStream.map(new StateFunction());

        // 键控状态，每个key保存一个状态，通过richfunction获取运行时上下文来获取状态
        userStream.keyBy(User::getName).flatMap(new WarnFlatMap(10.0));

        env.execute();
    }

//    private static class StateFunction implements MapFunction<User,Integer>,ListCheckpointed<Integer>  {
//        // 定义本地变量作为算子状态,跟key无关  掉电不存 实现ListCheckpointed来保证容错
//        private Integer count = 0;
//        @Override
//        public Integer map(User value) throws Exception {
//            count++;
//            return count;
//        }
//
//        @Override
//        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
//            return Collections.singletonList(count);
//        }
//
//        @Override
//        public void restoreState(List<Integer> state) throws Exception {
//            for (Integer integer : state) {
//                count += integer;
//            }
//        }
//    }

    private static class WarnFlatMap extends RichFlatMapFunction<User, Tuple3<String,Double,Double>> {
        private Double tempDiff;

        public WarnFlatMap(Double tempDiff) {
            this.tempDiff = tempDiff;
        }

        // 定义状态保存上次的温度
        private ValueState<Double> lastTemperatureState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemperatureState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemperature",Double.class));
        }

        @Override
        public void flatMap(User value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 获取状态
            Double lastTemp = lastTemperatureState.value();

            // 如果状态不为nul,判断两次温度差
            if (lastTemp != null) {
                Double diff = Math.abs(value.getTemperature() - lastTemp);
                if (diff>=tempDiff){
                    out.collect(new Tuple3<>(value.getName(),lastTemp,value.getTemperature()));
                }
            }else {
                // 更新温度
                lastTemperatureState.update(value.getTemperature());
            }
        }

        @Override
        public void close() throws Exception {
            lastTemperatureState.clear();
        }
    }
}
