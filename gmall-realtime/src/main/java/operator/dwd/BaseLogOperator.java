package operator.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import common.mapping.Constants;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CKUtils;
import common.conn.KafkaUtils;

/**
 * @author lihaoran
 * 数据流
 */
public class BaseLogOperator {
    private static final Logger logger = LoggerFactory.getLogger(BaseLogOperator.class.getSimpleName());

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为kafka分区数,多了没用
        env.setParallelism(1);

        //设置ck相关参数
        CKUtils.setCk(env);

        // 读取kafka数据
        DataStreamSource<String> logSource = env
                .addSource(KafkaUtils.makeKafkaConsumer(Constants.ODS_LOG_TOPIC, Constants.GROUP_LOG));

        //process将数据转为Json，遇到脏数据转到侧输出流 是因为有脏数据，可能解析不成，map必须有返回值，所以不用map转
        OutputTag<String> dirty = new OutputTag<String>("dirty") {
        };

        SingleOutputStreamOperator<JSONObject> jsonDataStream = logSource
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            // 解析json，解析不出来写到侧输出流
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            collector.collect(jsonObject);
                        } catch (Exception e) {
                            logger.warn("error data---> " + s);
                            context.output(dirty, s);
                        }
                    }
                });

        //按设备id分组，使用状态编程校验新老用户,老用户is_new为1的修改成0 效验
        SingleOutputStreamOperator<JSONObject> checkJsonDataStream = jsonDataStream
                .keyBy(json -> json.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    //定义状态
                    private ValueState<String> isNewState;

                    //初始化状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isNewState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<String>("isNew-state", String.class));
                    }

                    /**
                     * 校验是否为新用户
                     * @param jsonObject 数据
                     * @param context 上下文
                     * @param collector 收集器
                     * @throws Exception
                     */
                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                        // 取出数据中is_new字段
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");

                        // 如果为1则需要继续校验
                        if ("1".equals(isNew)) {
                            // 取出状态中的数据，判断是否为null
                            if (isNewState.value() != null) {
                                // 说明不是新用户
                                jsonObject.getJSONObject("common").put("is_new", "0");
                            } else {
                                // 说明为真正的新用户，更新state为一个不为null的值
                                isNewState.update("0");
                            }
                        }
                        //输出数据
                        collector.collect(jsonObject);
                    }
                });
        // 使用侧输出流将启动、曝光、页面数据分流，页面数据留在主流。曝光数据时页面数据一部分
        OutputTag<String> startOutputTag = new OutputTag<>("start");
        OutputTag<String> displayOutputTag = new OutputTag<>("display");
        SingleOutputStreamOperator<String> pageDataStream = checkJsonDataStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                // 获取启动数据
                String start = value.getString("start");
                // 检查一个字符串既不是null串也不是空串，要先检查str不为null，否则在一个null值上调length()方法会出现错误，或使用StringUtils.isNotBlank(start)
                if (start != null && start.length() > 0) {
                    // 启动数据
                    ctx.output(startOutputTag, value.toJSONString());
                } else {
                    // 不是启动数据就是页面数据
                    out.collect(value.toJSONString());

                    // 获取曝光数据,曝光数据是个json数组
                    JSONArray displays = value.getJSONArray("displays");

                    // 取出公共字段，页面信息，时间戳，转为String会添加很多转义符
                    JSONObject common = value.getJSONObject("common");
                    JSONObject page = value.getJSONObject("page");
                    Long ts = value.getLong("ts");

                    // 判断曝光数据是否存在，封装数据
                    if (displays != null && displays.size() > 0) {
                        JSONObject displayObject = new JSONObject();
                        displayObject.put("common", common);
                        displayObject.put("page", page);
                        displayObject.put("ts", ts);
                        // 遍历每条曝光数据
                        for (Object display : displays) {
                            // 过来一条数据，添加一条
                            displayObject.put("display", display);
                            ctx.output(displayOutputTag, displayObject.toJSONString());
                        }
                    }
                }
            }
        });
        // 获取各流
        jsonDataStream.getSideOutput(dirty).print("dirty");
        pageDataStream.print("page>>>>>>>>");
        DataStream<String> startDataStream = pageDataStream.getSideOutput(startOutputTag);
        startDataStream.print("start>>>>>>>>");
        DataStream<String> displayDataStream = pageDataStream.getSideOutput(displayOutputTag);
        displayDataStream.print("display>>>>>>>>");

        // 写入kafka
        pageDataStream.addSink(KafkaUtils.makeKafkaProducer(Constants.DWD_PAGE_TOPIC));
        startDataStream.addSink(KafkaUtils.makeKafkaProducer(Constants.DWD_START_TOPIC));
        displayDataStream.addSink(KafkaUtils.makeKafkaProducer(Constants.DWD_DISPLAY_TOPIC));
        env.execute();
    }
}
