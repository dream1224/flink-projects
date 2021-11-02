package operator.dws;

import bean.VisitorStats;
import com.alibaba.fastjson.JSONObject;
import common.mapping.Constants;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import common.conn.KafkaUtils;

import java.time.Duration;

public class VisitOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


//        CKUtils.setCk(env);

        // 读取kafka数据
        DataStreamSource<String> pageStream = env.addSource(KafkaUtils.makeKafkaConsumer(Constants.DWD_PAGE_TOPIC, Constants.VISITOR_GROUP));
        DataStreamSource<String> uvStream = env.addSource(KafkaUtils.makeKafkaConsumer(Constants.DWM_UV_TOPIC, Constants.VISITOR_GROUP));
        DataStreamSource<String> jumpStream = env.addSource(KafkaUtils.makeKafkaConsumer(Constants.DWM_USER_JUMP_DETAIL_TOPIC, Constants.VISITOR_GROUP));

        // 将各流转换为统一格式
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPV = pageStream.map(line -> {
            // 将数据转换为Json对象
            JSONObject jsonObject = JSONObject.parseObject(line);
            // 取出common字段
            JSONObject common = jsonObject.getJSONObject("common");
            // 取出上一跳页面信息
            String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
            Long sv = 0L;
            if (lastPageId == null || lastPageId.length() <= 0) {
                sv = 1L;
            }

            // 封装统一格式对象并返回
            return new VisitorStats("", "",
                    common.getString("vc"), common.getString("ch"), common.getString("ar"), common.getString("is_new"), 0L, 1L, sv, 0L, jsonObject.getJSONObject("page").getLong("during_time"), jsonObject.getJSONObject("ts").getLong("ts"));
        });

        // 处理UV流
        SingleOutputStreamOperator<VisitorStats> visitorWithUV = uvStream.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "", common.getString("vc"), common.getString("ch"), common.getString("ar"), common.getString("is_new"), 1L, 0L, 0L, 0L, 0L, jsonObject.getJSONObject("ts").getLong("ts"));
        });

        // 处理跳出流
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithJump = jumpStream.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "", common.getString("vc"), common.getString("ch"), common.getString("ar"), common.getString("is_new"), 0L, 0L, 0L, 1L, 0L, jsonObject.getJSONObject("ts").getLong("ts"));
        });

        // union各流并提取时间戳生成watermark
        DataStream<VisitorStats> visitorStatsDataStream = visitorStatsWithPV
                .union(visitorWithUV, visitorStatsWithJump).assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // 按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsKeyedStream = visitorStatsDataStream.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return new Tuple4<>(visitorStats.getVc(), visitorStats.getCh(), visitorStats.getAr(), visitorStats.getIs_new());
            }
        });
        // 聚合
        visitorStatsKeyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                        return null;
                    }
                }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                        long start = window.getStart();
                        long end = window.getEnd();
                    }
                });

        // 将数据写出到Clickhouse


        // 启动
        env.execute();

    }
}
