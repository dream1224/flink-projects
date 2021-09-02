package common.serialization;

import com.alibaba.fastjson.JSONObject;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class UDFSerialization implements com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema<String> {

    /**
     * 将CDC采集到的数据序列化成标准格式
     *
     * @param sourceRecord CDC采集到的数据
     * @param collector    输出
     * @throws Exception
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        // 获取topic,db,table,value
        String[] topic = sourceRecord.topic().split("\\.");
        String db = topic[1];
        String table = topic[2];
        Struct value = (Struct) sourceRecord.value();
        Envelope.Operation operationType = Envelope.operationFor(sourceRecord);
        String operation = operationType.toString().toLowerCase();
        //获取操作类型
        JSONObject result = new JSONObject();
        result.put("database", db);
        result.put("table", table);
        if ("create".equals(operation)) {
            operation = "insert";
        } else if ("d".equals(operation)) {
            operation = "delete";
        }
        result.put("type", operation);
        result.put("ts", System.currentTimeMillis());
        if ("delete".equals(operation)) {
            Struct before = value.getStruct("before");
            // 将获取到的topic,db,table,value包装成json格式
            JSONObject jsonObject = new JSONObject();
            for (Field field : before.schema().fields()) {
                Object o = before.get(field);
                jsonObject.put(field.name(), o);
            }
            result.put("data", jsonObject);
        } else {
            Struct after = value.getStruct("after");
            // 将获取到的topic,db,table,value包装成json格式
            JSONObject jsonObject = new JSONObject();
            for (Field field : after.schema().fields()) {
                Object o = after.get(field);
                jsonObject.put(field.name(), o);
            }
            result.put("data", jsonObject);
        }
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
