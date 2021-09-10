package operator.dwm;

import bean.OrderDetail;
import bean.OrderInfo;
import bean.OrderWide;
import com.alibaba.fastjson.JSON;
import common.Constants;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CKUtils;
import utils.DimUtils;
import utils.KafkaUtils;
import utils.PhoenixUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class OrderWideOperator {
    private static Logger logger = LoggerFactory.getLogger(OrderWideOperator.class);

    public static void main(String[] args) throws Exception {
        // TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 开启CK
//        CKUtils.setCk(env);

        // TODO 读取Kafka dwd_order_info和dwd_order_detail主题的数据
        DataStreamSource<String> orderInfoStrStream = env.addSource(KafkaUtils.makeKafkaConsumer(Constants.KAFKA_DWD_ORDER_INFO_TOPIC, Constants.ORDER_WIDE_GROUP));
        DataStreamSource<String> orderDetailStrStream = env.addSource(KafkaUtils.makeKafkaConsumer(Constants.KAFKA_DWD_ORDER_DETAIL_TOPIC, Constants.ORDER_WIDE_GROUP));

        // TODO 将两个流转换为POJO类型，并提取时间戳生成watermark
        SingleOutputStreamOperator<OrderInfo> orderInfoWatermarkStream = orderInfoStrStream.map(line -> {
            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
            // 给create_date,create_hour,create_ts赋值
            // yyyy-MM-dd HH:mm:ss
            String create_time = orderInfo.getCreate_time();
            String[] dateArray = create_time.split(" ");
            orderInfo.setCreate_date(dateArray[0]);
            orderInfo.setCreate_hour(dateArray[0].split(":")[0]);
            // SimpleDateFormat不能移出方法外，因为format和parse操作了公共资源，有线程安全问题
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long ts = dateFormat.parse(create_time).getTime();
            orderInfo.setCreate_ts(ts);
            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailWatermarkStream = orderDetailStrStream.map(line -> {
            OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
            // 给create_ts赋值
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long ts = dateFormat.parse(orderDetail.getCreate_time()).getTime();
            orderDetail.setCreate_ts(ts);
            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        // TODO 将两个流join
        SingleOutputStreamOperator<OrderWide> orderWideStream = orderInfoWatermarkStream.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWatermarkStream.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        // TODO join其他维度
        // 用户维度
        orderWideStream.map(new RichMapFunction<OrderWide, OrderWide>() {
            //定义Phoenix连接
            private Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                Class.forName(Constants.PHOENIX_DRIVER);
                try {
                    connection = DriverManager.getConnection(Constants.PHOENIX_SERVER);
                } catch (SQLException e) {
                    this.open(parameters);
                    e.printStackTrace();
                }
            }

            @Override
            public void close() throws Exception {
                if (connection != null) {
                    connection.close();
                }
            }

            @Override
            public OrderWide map(OrderWide orderWide) throws Exception {
                Long user_id = orderWide.getUser_id();

                // 查询Phoenix
                try {
                    DimUtils.getDimInfo(connection, "DIM_USER_INFO", user_id.toString());
                } catch (Exception e) {
                    System.out.println(user_id + "is not exist in" + "DIM_USER_INFO");
                    logger.error("data is not exist");
                    e.printStackTrace();
                }
                return null;
            }
        });

        // 地区维度

        // SKU维度

        // SPU维度

        // Category维度

        // Trademark维度

        // TODO 将宽表写入Kafka

        // TODO 执行
        env.execute();


    }
}
