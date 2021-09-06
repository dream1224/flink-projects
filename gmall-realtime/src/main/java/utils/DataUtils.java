package utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DataUtils {
    private static final Logger logger = LoggerFactory.getLogger(DataUtils.class);

    /**
     * 拼接主键
     *
     * @param value
     * @param sinkPk
     * @return
     */
    public static String makePk(JSONObject value, String sinkPk) {
        JSONObject data = value.getJSONObject("data");
        HashMap<String, Object> dataMap = new HashMap<>();
        Collection<Object> pkValues = null;

        // TODO 遍历JSON，将key和value写入Map
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            dataMap.put(next.getKey(), next.getValue());
        }

        // TODO 切割sinkPk,获取主键各部分的列名，根据列名从Map中获取对应的值拼接成主键
        String[] columns = sinkPk.split(",");
        for (String column : columns) {
            Object values = dataMap.get(column);
            pkValues.add(values);
        }
        return StringUtils.join(pkValues, "-");
    }
}
