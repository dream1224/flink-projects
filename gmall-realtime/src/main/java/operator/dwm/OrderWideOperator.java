package operator.dwm;

import bean.OrderDetail;
import bean.OrderInfo;
import bean.OrderWide;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.mapping.Constants;
import common.process.DimAsyncFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import common.conn.KafkaUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class OrderWideOperator {
    private static Logger logger = LoggerFactory.getLogger(OrderWideOperator.class);

    public static void main(String[] args) throws Exception {
        // TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 开启CK
//        CKUtils.setCk(env);

        // TODO 读取Kafka dwd_order_info和dwd_order_detail主题的数据
        DataStreamSource<String> orderInfoStrStream = env.addSource(KafkaUtils.makeKafkaConsumer(Constants.DWD_ORDER_INFO_TOPIC, Constants.ORDER_WIDE_GROUP));
        DataStreamSource<String> orderDetailStrStream = env.addSource(KafkaUtils.makeKafkaConsumer(Constants.DWD_ORDER_DETAIL_TOPIC, Constants.ORDER_WIDE_GROUP));

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
//        // 用户维度
//        orderWideStream.map(new RichMapFunction<OrderWide, OrderWide>() {
//            //定义Phoenix连接
//            private Connection connection;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                Class.forName(Constants.PHOENIX_DRIVER);
//                try {
//                    connection = DriverManager.getConnection(Constants.PHOENIX_SERVER);
//                } catch (SQLException e) {
//                    this.open(parameters);
//                    e.printStackTrace();
//                }
//            }
//
//            @Override
//            public void close() throws Exception {
//                if (connection != null) {
//                    connection.close();
//                }
//            }
//
//            /**
//             * map为同步处理方式，前一条处理完之后，后一条才开始处理
//             * @param orderWide
//             * @return
//             * @throws Exception
//             */
//            @Override
//            public OrderWide map(OrderWide orderWide) throws Exception {
//                Long user_id = orderWide.getUser_id();
//
//                // 查询Phoenix
//                try {
//                    JSONObject dim_user_info = DimUtils.getDimInfo(connection, "DIM_USER_INFO", user_id.toString());
//
//                } catch (Exception e) {
//                    System.out.println(user_id + "is not exist in" + "DIM_USER_INFO");
//                    logger.error("data is not exist");
//                    e.printStackTrace();
//                }
//                return null;
//            }
//        });
        /**
         * unorderedWait 无序模式 异步请求一结束就立刻发出结果记录。
         */
        SingleOutputStreamOperator<OrderWide> orderWideWithUser = AsyncDataStream.unorderedWait(orderWideStream, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
            /**
             * 补充维度信息
             *
             * @param orderWide
             * @param dimInfo
             */
            @Override
            public void joinDim(OrderWide orderWide, JSONObject dimInfo) {
                String birthday = dimInfo.getString("BIRTHDAY");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                // 获取日历对象
                Calendar nowTime = Calendar.getInstance();
                Calendar bornTime = Calendar.getInstance();
                // 获取当前时间
                long ts = System.currentTimeMillis();
                long time = ts;
                nowTime.setTimeInMillis(ts);
                try {
                    // 获取生日时间
                    time = sdf.parse(birthday).getTime();
                    bornTime.setTimeInMillis(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                    logger.error("parse birthday error" + birthday);
                }

                if (bornTime.after(nowTime) || nowTime.equals(bornTime)) {
                    throw new IllegalArgumentException("born time error");
                }

                int age = nowTime.get(Calendar.YEAR) - bornTime.get(Calendar.YEAR);
                if (nowTime.get(Calendar.DAY_OF_YEAR) < bornTime.get(Calendar.DAY_OF_YEAR)) {
                    age -= 1;
                }

                // 获取性别
                String gender = dimInfo.getString("GENDER");

                // 构建宽表对象
                orderWide.setUser_age(age);
                orderWide.setUser_gender(gender);
            }

            /**
             * 获取PK
             *
             * @param input
             * @return
             */
            @Override
            public String getPk(OrderWide input) {
                return input.getUser_id().toString();
            }
        }, 60, TimeUnit.SECONDS);

        // 地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvince = AsyncDataStream.unorderedWait(orderWideWithUser, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public void joinDim(OrderWide input, JSONObject dimInfo) {
                String name = dimInfo.getString("NAME");
                String area_code = dimInfo.getString("AREA_CODE");
                String iso_code = dimInfo.getString("ISO_CODE");
                String iso_3166_2 = dimInfo.getString("ISO_3166_2");
                input.setProvince_name(name);
                input.setProvince_area_code(area_code);
                input.setProvince_iso_code(iso_code);
                input.setProvince_3166_2_code(iso_code);
            }

            @Override
            public String getPk(OrderWide input) {
                return input.getProvince_id().toString();
            }
        }, 60, TimeUnit.SECONDS);

        // SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSku = AsyncDataStream.unorderedWait(orderWideWithProvince,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void joinDim(OrderWide input, JSONObject dimInfo) {
                        String sku_name = dimInfo.getString("SKU_NAME");
                        Long category3_id = dimInfo.getLong("CATEGORY3_ID");
                        Long spu_id = dimInfo.getLong("SPU_ID");
                        Long tm_id = dimInfo.getLong("TM_ID");
                        input.setSku_name(sku_name);
                        input.setCategory3_id(category3_id);
                        input.setSpu_id(spu_id);
                        input.setTm_id(tm_id);
                    }

                    @Override
                    public String getPk(OrderWide input) {
                        return input.getSku_id().toString();
                    }
                }, 60, TimeUnit.SECONDS
        );

        // SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpu = AsyncDataStream.unorderedWait(orderWideWithSku, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
            @Override
            public void joinDim(OrderWide input, JSONObject dimInfo) {
                String spu_name = dimInfo.getString("SPU_NAME");
                input.setSpu_name(spu_name);
            }

            @Override
            public String getPk(OrderWide input) {
                return input.getSpu_id().toString();
            }
        }, 60, TimeUnit.SECONDS);

        // Trademark维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTm = AsyncDataStream.unorderedWait(orderWideWithSpu, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
            @Override
            public void joinDim(OrderWide input, JSONObject dimInfo) {
                String tm_name = dimInfo.getString("TM_NAME");
                input.setTm_name(tm_name);
            }

            @Override
            public String getPk(OrderWide input) {
                return input.getTm_id().toString();
            }
        }, 60, TimeUnit.SECONDS);

        // Category维度
        SingleOutputStreamOperator<OrderWide> orderWideAll = AsyncDataStream.unorderedWait(orderWideWithTm,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void joinDim(OrderWide input, JSONObject dimInfo) {
                        String name = dimInfo.getString("NAME");
                        input.setCategory3_name(name);
                    }

                    @Override
                    public String getPk(OrderWide input) {
                        return input.getCategory3_id().toString();
                    }
                }, 60, TimeUnit.SECONDS);

        // TODO 将宽表写入Kafka
        orderWideAll.map(orderWide -> JSONObject.toJSONString(orderWide))
                .addSink(KafkaUtils.makeKafkaProducer(Constants.DWM_ORDER_WIDE));

        // TODO 执行
        env.execute();


    }
}
