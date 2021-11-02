package operator.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import common.mapping.Constants;
import common.serialization.UDFSerialization;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import common.conn.KafkaUtils;

/**
 * @author lihaoran
 */
public class FlinkCdcSource {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启CK
//        CKUtils.setCk(env);

        // 使用CDC作为Source读取mysql变化数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource
                .<String>builder()
                .hostname(Constants.HOSTNAME)
                .port(Constants.PORT)
                .username(Constants.USERNAME)
                .password(Constants.PASSWORD)
                .databaseList("gmall2021")
                .startupOptions(StartupOptions.latest())
                .deserializer(new UDFSerialization())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
        dataStreamSource.print();
        dataStreamSource.addSink(KafkaUtils.makeKafkaProducer(Constants.ODS_DB_TOPIC));
        env.execute();
    }
}
