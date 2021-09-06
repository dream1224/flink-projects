package common.sink;

import com.alibaba.fastjson.JSONObject;
import common.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.PhoenixUtils;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;


@SuppressWarnings("Duplicates")
public class hbaseSinkFunction extends RichSinkFunction<JSONObject> {
    private static final Logger logger = LoggerFactory.getLogger(hbaseSinkFunction.class);
    //定义Phoenix连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化连接
        Class.forName(Constants.PHOENIX_DRIVER);
        try {
            connection = DriverManager.getConnection(Constants.PHOENIX_SERVER);
        } catch (SQLException e) {
            logger.warn("get phoenix connection failure,reset connection");
            e.printStackTrace();
            this.open(parameters);
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }


    /**
     * @param value   {"id":"","name":"","sinkTable":""}  -> {"data":{"id":"","name":""},"sinkTable": ""}
     * @param context 上下文
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 准备写入数据的sql
        JSONObject data = value.getJSONObject("data");
        String sinkTable = value.getString("sinkTable");
        String type = value.getString("type");
        String pkValues = value.getString("pk");

        if (!"delete".equals(type)) {
            String upsertSql = createUpsertSql(sinkTable, data, pkValues);
            System.out.println("upsertSql>>>>>>" + upsertSql);
            logger.info("upsertSql is>>>>>>" + upsertSql);
            // 执行插入语句
            PhoenixUtils.executeSql(connection, upsertSql);
        } else {
            String deleteSql = createDeleteSql(sinkTable, pkValues);
            System.out.println("deleteSql>>>>>>" + deleteSql);
            // 执行删除语句
            PhoenixUtils.executeSql(connection, deleteSql);
        }
    }

    /**
     * 生成delete Sql
     *
     * @param sinkTable
     * @return
     */
    private String createDeleteSql(String sinkTable, String pkValues) {
        return "delete from "
                + Constants.HBASE_SCHEME
                + "."
                + sinkTable
                + " where pk = "
                + pkValues;
    }


    /**
     * 生成upsert sql  upsert into xx.xx(id,name) values('','')
     *
     * @param sinkTable 表名
     * @param data      数据
     */
    private String createUpsertSql(String sinkTable, JSONObject data, String pkValues) {
        // 取出JSON中的key和value
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into "
                + Constants.HBASE_SCHEME
                + "."
                + sinkTable + "("
                + "pk,"
                + StringUtils.join(keySet, ",")
                + ") values('"
                + pkValues
                + "','"
                + StringUtils.join(values, "','")
                + "')";
    }
}
