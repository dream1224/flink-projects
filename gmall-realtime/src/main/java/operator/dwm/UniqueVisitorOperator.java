package operator.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.Constants;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hdfs.util.Diff;
import org.apache.kafka.common.protocol.types.Field;
import utils.KafkaUtils;

import java.text.SimpleDateFormat;

public class UniqueVisitorOperator {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取kafka dwd_page_log 主题数据
        DataStreamSource<String> DwdPageLogStream = env.addSource(KafkaUtils.makeKafkaConsumer(Constants.KAFKA_DWD_PAGE_TOPIC, Constants.UV_GROUP_ID));

        // 将数据转为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonStream = DwdPageLogStream.map(JSON::parseObject);

        // 按照mid分组
        KeyedStream<JSONObject, String> keyedJsonStream = jsonStream.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // 使用状态编程的方式过过滤数据
        SingleOutputStreamOperator<JSONObject> uvDataStream = keyedJsonStream.filter(new RichFilterFunction<JSONObject>() {
            // 定义状态
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitState", String.class);
                // 设置TTL，状态过时
                StateTtlConfig stateTtlConfigBuild = new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfigBuild);

                this.lastVisitState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 取出当前状态数据
                String lastVisit = lastVisitState.value();

                // 取出当前数据中的时间
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                String currentDate = simpleDateFormat.format(value.getLong("ts"));
                if (lastVisit == null || !lastVisit.equals(currentDate)) {
                    // 今天的新访问数据
                    lastVisitState.update(currentDate);
                    return true;
                } else {
                    return false;
                }
            }
        });

        // 写入kafka
        uvDataStream.map(json -> json.toJSONString()).addSink(KafkaUtils.makeKafkaProducer(Constants.KAFKA_DWM_UV_TOPIC));

        // 启动任务
        env.execute();
    }

}
