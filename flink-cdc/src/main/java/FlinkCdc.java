import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.google.gson.JsonObject;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @author lihaoran
 */
public class FlinkCdc {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**开启ck
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        env.setStateBackend(new FsStateBackend("hdfs://cluster001:8020/flinkdata/checkpoint"));
        */
        //使用cdc读取mysql数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("cluster001")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2021")
                .tableList("gmall2021.base_trademark")
                .startupOptions(StartupOptions.initial())
//                .deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new PersonalDefineDeserializationSchema())
                .build();
        //获取流
        DataStreamSource<String> source = env.addSource(sourceFunction);
        //打印
        source.print();
        //执行
        env.execute();
    }


    private static class PersonalDefineDeserializationSchema implements com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema<String> {
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            String[] topic = sourceRecord.topic().split("\\.");
            String db = topic[1];
            String table = topic[2];
            Struct value = (Struct) sourceRecord.value();
            Struct after = value.getStruct("after");
            JSONObject jsonObject = new JSONObject();
            for (Field field : after.schema().fields()) {
                Object o = after.get(field);
                jsonObject.put(field.name(),o);
            }

            //获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            JSONObject result = new JSONObject();
            result.put("database",db);
            result.put("table",table);
            result.put("data",jsonObject);
            result.put("op",operation);
            result.put("ts",System.currentTimeMillis());
            collector.collect(result.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
