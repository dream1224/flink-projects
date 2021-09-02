package common.sink;

import com.alibaba.fastjson.JSONObject;
import common.Constants;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;


@SuppressWarnings("Duplicates")
public class hbaseSinkFunction extends RichSinkFunction<JSONObject> {
    private static final Logger logger = LoggerFactory.getLogger(hbaseSinkFunction.class);
    //定义Phoenix连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化连接
        Class.forName(Constants.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(Constants.PHOENIX_SERVER);
        if (connection != null) {
            System.out.println("获取Phoenix连接正常");
            logger.info("get phoenix connection success");
        } else {
            System.out.println("获取Phoenix连接异常");
            logger.warn("get phoenix connection failure,reset connection");
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
        String upsertSql = createUpsertSql(sinkTable, data);
        System.out.println("upsertSql>>>>>" + upsertSql);
        logger.info("upsertSql is>>>>>>" + upsertSql);
        // 预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(upsertSql);

        // 执行写入数据操作 维度表数据量小可以不用批处理(通过数量或者时间)，历史数据初始化
//        preparedStatement.addBatch();
        preparedStatement.execute();
        connection.commit();
        // 新开一个线程，定时调度
//        Timer timer = new Timer();
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                try {
//                    connection.commit();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        }, 5000l, 1000);
    }

    /**
     * 生成upsert sql  upsert into xx.xx(id,name) values('','')
     *
     * @param sinkTable 表名
     * @param data      数据
     */
    private String createUpsertSql(String sinkTable, JSONObject data) {
        // 取出JSON中的key和value
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into "
                + Constants.HBASE_SCHEME
                + "."
                + sinkTable + "("
                + StringUtils.join(keySet, ",")
                + ") values('"
                + StringUtils.join(values, "','")
                + "')";
    }
}
