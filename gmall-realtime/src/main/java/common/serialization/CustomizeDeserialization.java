package common.serialization;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @author lihaoran
 */
public class CustomizeDeserialization implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        String[] topic = sourceRecord.topic().split("\\.");
        String db = topic[1];
        String table = topic[2];

        // 获取数据
        Struct value = (Struct) sourceRecord.value();
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            for (Field field : after.schema().fields()) {
                Object o = after.get(field);
                afterJson.put(field.name(), o);
            }
        }

        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            for (Field field : before.schema().fields()) {
                Object o = before.get(field);
                beforeJson.put(field.name(), o);
            }
        }

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        JSONObject result = new JSONObject();
        result.put("database", db);
        result.put("table", table);
        result.put("before-data", beforeJson);
        result.put("after-data", afterJson);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }
        result.put("type", type);
        result.put("ts", System.currentTimeMillis());
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
