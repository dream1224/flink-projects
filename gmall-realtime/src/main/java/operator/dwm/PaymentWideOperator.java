package operator.dwm;


import bean.OrderWide;
import bean.PaymentInfo;
import bean.PaymentWide;
import com.alibaba.fastjson.JSONObject;
import common.mapping.Constants;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

public class PaymentWideOperator {
    private Logger logger = LoggerFactory.getLogger(PaymentWideOperator.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        CKUtils.setCk(env);
        // 读取订单宽表数据
        DataStreamSource<String> orderWideStream = env.addSource(KafkaUtils.makeKafkaConsumer(Constants.DWM_ORDER_WIDE, Constants.GROUP_ORDER_WIDE));

        // 读取支付表数据
        DataStreamSource<String> paymentStream = env.addSource(KafkaUtils.makeKafkaConsumer(Constants.DWD_PAYMENT_WIDE, Constants.GROUP_PAYMENT));

        // 将两个流转换为POJO类型，并提取时间戳，生成watermark
        SingleOutputStreamOperator<OrderWide> orderWideWatermark = orderWideStream.map(orderWide -> JSONObject.parseObject(orderWide, OrderWide.class)).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
            @Override
            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                String create_time = element.getCreate_time();
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    return simpleDateFormat.parse(create_time).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                    return recordTimestamp;
                }
            }
        }));

        SingleOutputStreamOperator<PaymentInfo> paymentWatermark = paymentStream.map(payment -> JSONObject.parseObject(payment, PaymentInfo.class)).assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
            @Override
            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                String create_time = element.getCreate_time();
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    return simpleDateFormat.parse(create_time).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                    return recordTimestamp;
                }
            }
        }));

        // 将两个流join成支付宽表  订单等支付
        SingleOutputStreamOperator<PaymentWide> paymentWideStream = paymentWatermark.keyBy(orderWide -> orderWide.getOrder_id()).intervalJoin(orderWideWatermark.keyBy(payment -> payment.getOrder_id())).between(Time.minutes(-15), Time.minutes(0)).process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
            @Override
            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                out.collect(new PaymentWide(paymentInfo, orderWide));
            }
        });

        // 写入kafka
        paymentWideStream.map(paymentWide -> JSONObject.toJSONString(paymentStream)).addSink(KafkaUtils.makeKafkaProducer(Constants.DWM_PAYMENT_WIDE));

        //启动执行
        env.execute();
    }
}
