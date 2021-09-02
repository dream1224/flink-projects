package window;

import bean.PhoneDetail;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author lihaoran
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


//        DataStream<String> dataStreamSource = env.readTextFile("/Users/lihaoran/Documents/flink-projects/flink-test/src/main/resources/phone.txt");
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        DataStream<PhoneDetail> streamOperator = dataStreamSource.map(new MapFunction<String, PhoneDetail>() {
            @Override
            public PhoneDetail map(String s) throws Exception {
                String[] split = s.split(",");
                return new PhoneDetail(split[0], new Double(split[1]));
            }
        });


        // 按操作流分类分为 window(Keyedstream)和windowAll(Datastream)
        // 全窗口函数1
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> apply = streamOperator.keyBy(PhoneDetail -> PhoneDetail.getPhoneName()).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).apply(new WindowFunction<PhoneDetail, Tuple3<String, Long, Integer>, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<PhoneDetail> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                String id = s;
                Long windowEnd = window.getEnd();
                Integer size = IteratorUtils.toList(input.iterator()).size();
                out.collect(new Tuple3<>(id, windowEnd, size));
            }
        });
        apply.print();

        // 全窗口函数2
//        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> process = streamOperator.keyBy(PhoneDetail -> PhoneDetail.getPhoneName()).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).process(new ProcessWindowFunction<PhoneDetail, Tuple3<String, Long, Integer>, String, TimeWindow>() {
//            @Override
//            public void process(String s, Context context, Iterable<PhoneDetail> elements, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
//                String id = s;
//                Long windowEnd = context.currentProcessingTime();
//                Integer size = IteratorUtils.toList(elements.iterator()).size();
//                out.collect(new Tuple3<>(id, windowEnd, size));
//            }
//        });
//
//        process.print();

        // 对于Keyedstream的窗口来说，他可以使得多任务并行计算，每一个logical key stream将会被独立的进行处理。windowAll把所有数据放到同一个窗口

        // 按窗口功能分类 分为时间窗口timeWindow和计数窗口countWindow

        // 按照窗口的Assigner来分 滚动窗口 滑动窗口 会话窗口 全局窗口
        // 滚动窗口
        OutputTag<PhoneDetail> outputTag = new OutputTag<>("data");
        SingleOutputStreamOperator<Tuple2<String, Integer>> aggregate = streamOperator
                .keyBy(phoneDetail -> phoneDetail.getPhoneName())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .trigger()
//                .evictor()
//                .allowedLateness()
                //Setting an allowed lateness is only valid for event-time windows.
                .sideOutputLateData(outputTag)
                .aggregate(new AggregateFunction<PhoneDetail, phoneRate, Tuple2<String, Integer>>() {
            @Override
            public phoneRate createAccumulator() {
                return new phoneRate();
            }

            @Override
            public phoneRate add(PhoneDetail value, phoneRate accumulator) {
                accumulator.phoneName = value.getPhoneName();
                accumulator.count++;
                return accumulator;
            }

            @Override
            public Tuple2<String, Integer> getResult(phoneRate accumulator) {
                return new Tuple2(accumulator.phoneName, accumulator.count);
            }

            @Override
            public phoneRate merge(phoneRate a, phoneRate b) {
                a.count += b.count;
                return a;
            }
        });

        DataStream<PhoneDetail> sideOutput = aggregate.getSideOutput(outputTag);

//        aggregate.print();

        // 滑动窗口
//        streamOperator.keyBy(phoneDetail -> phoneDetail.getPhoneName()).window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(1),Time.hours(-8)));
//
//        // 会话窗口
//        streamOperator.keyBy(phoneDetail -> phoneDetail.getPhoneName()).window(EventTimeSessionWindows.withGap(Time.seconds(10)));
//
//        // 全局窗口 将所有相同keyed的元素分配到一个窗口里
//        streamOperator.keyBy(phoneDetail -> phoneDetail.getPhoneName()).window(GlobalWindows.create());


        env.execute();

    }

    private static class phoneRate {
        String phoneName;
        int count;
    }
}


