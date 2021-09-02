package window;

import bean.User;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter;
import org.apache.flink.util.OutputTag;

/**
 * @author lihaoran
 */
public class EventTimeTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.12默认EventTime In Flink 1.12 the default stream time characteristic has been changed to TimeCharacteristic#EventTime 分布式，网络 -> 乱序
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置时间间隔
//        env.getConfig().setAutoWatermarkInterval();
//        DataStream<String> dataStream = env.readTextFile("/Users/lihaoran/Documents/flink-projects/flink-test/src/main/resources/User.txt");

        // 离源端越近设置watermark越好，由于受到watermark在多分区传递的影响
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<User> userStream = dataStream.map(new MapFunction<String, User>() {
            @Override
            public User map(String value) throws Exception {
                String[] split = value.split(",");
                return new User(split[0], new Double(split[1]), new Long(split[2]));
            }
        })
                //设置water与时间戳 Please use assignTimestampsAndWatermarks(WatermarkStrategy) instead.
                // 没有乱序，升序数据设置事件时间和watermark
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<User>() {
//                    @Override
//                    public long extractAscendingTimestamp(User element) {
//                        return element.getTime();
//                    }
//                })
                // AssignerWithPunctuatedWatermarks间断式地生成watermark。和周期性生成的方式不同，这种方式不是固定时间的，而是可以根据需要对每条数据进行筛选和处理，数据量大时性能低
//                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<User>() {
//                    @Override
//                    public Watermark checkAndGetNextWatermark(User lastElement, long extractedTimestamp) {
//                        return null;
//                    }
//
//                    @Override
//                    public long extractTimestamp(User element, long recordTimestamp) {
//                        return 0;
//                    }
//                })
                // 乱序数据设置事件时间和watermark BoundedOutOfOrdernessTimestampExtractor 周期性的生成watermark：系统会周期性的将watermark插入到流中(水位线也是一种特殊的事件!)。默认周期是200毫秒，数据量小时浪费
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<User>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(User element) {
                        // 毫秒值
                        return element.getTime()*1000L;
                    }
                });
        OutputTag outputTag = new OutputTag<User>("late");
        SingleOutputStreamOperator<User> minStream = userStream.keyBy(User::getName)
                // 窗口起始点 窗口大小的整数倍 timestamp - (timestamp - offset + windowSize) % windowSize offset为窗口偏移量，一般处理不同时区的时间
                // 三重保证 watermark allowedLateness 侧输出流
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(10))
                .sideOutputLateData(outputTag)
                .minBy("temperature");
        DataStream sideOutput = minStream.getSideOutput(outputTag);
        sideOutput.print("late");
        minStream.print("minAge");
        env.execute();
    }
}
