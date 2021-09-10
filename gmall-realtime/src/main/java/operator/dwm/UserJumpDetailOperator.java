package operator.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.Constants;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import utils.CKUtils;
import utils.KafkaUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@SuppressWarnings("ALL")
public class UserJumpDetailOperator {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启CK
//        CKUtils.setCk(env);

        // 读取Kafka dwd_page_log主题数据
        DataStreamSource<String> pageStream = env.addSource(KafkaUtils.makeKafkaConsumer(Constants.KAFKA_DWD_PAGE_TOPIC, Constants.USER_JUMP_DETAIL_GROUP_ID));

        // 转换为JSON对象并提取数据中的时间戳生成watermark,防止任务挂掉，造成影响
        SingleOutputStreamOperator<JSONObject> pageJsonStream = pageStream.map(json -> JSON.parseObject(json));
        WatermarkStrategy<JSONObject> watermarkStrategy = WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                });
        SingleOutputStreamOperator<JSONObject> watermarkStream = pageJsonStream.assignTimestampsAndWatermarks(watermarkStrategy);

        // 按照mid分组
        KeyedStream<JSONObject, String> keyedJsonStream = watermarkStream.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // 定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        // 提取数据中上一条页面
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                }).times(2)
                .consecutive()
                .within(Time.seconds(10));

        // 将模式序列作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedJsonStream, pattern);

        // 提取事件(包含超时事件)
        OutputTag<String> outputTag = new OutputTag<String>("timeout") {
        };
        SingleOutputStreamOperator<String> selectDataStream = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, String>() {
            // 处理超时事件
            @Override
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                JSONObject start = map.get("start").get(0);
                return start.toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            // 处理匹配上的数据
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                JSONObject start = map.get("start").get(0);
                return start.toJSONString();
            }
        });

        // 合并主流(匹配上的事件)和侧输出流(超时事件)
        DataStream<String> sideOutput = selectDataStream.getSideOutput(outputTag);
        DataStream<String> dataStream = selectDataStream.union(sideOutput);

        // 写入Kafka
        dataStream.print();
        dataStream.addSink(KafkaUtils.makeKafkaProducer(Constants.KAFKA_DWM_USER_JUMP_DETAIL_TOPIC));

        env.execute();
    }
}
