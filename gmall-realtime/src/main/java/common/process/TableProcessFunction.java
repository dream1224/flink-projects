package common.process;

import bean.TableConfig;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.Constants;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * * @param <IN1> The input type of the non-broadcast side.
 * * @param <IN2> The input type of the broadcast side.
 * * @param <OUT> The output type of the operator.
 */
@SuppressWarnings("Duplicates")
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    // 定义日志打印器
    private final Logger logger = LoggerFactory.getLogger(TableProcessFunction.class);
    // 定义phoenix连接
    private Connection connection;
    // 定义OutputTag
    private OutputTag<JSONObject> outputTag;
    // 定义Map状态描述器
    private MapStateDescriptor<String, TableConfig> mapStateDescriptor;

    public TableProcessFunction() {
    }

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableConfig> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
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
     * 处理传过来的广播数据
     *
     * @param value 数据
     * @param ctx   上下文
     * @param out   输出
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        // TODO 将数据转为javaBean
        // 将字符串转为JSON
        JSONObject jsonObject = JSONObject.parseObject(value);

        // 获取多级JSON中的data
        JSONObject data = jsonObject.getJSONObject("data");

        // 将data转为POJO类型，自动转换
        TableConfig TableConfig = JSON.parseObject(data.toJSONString(), TableConfig.class);

        if (TableConfig != null) {
            // TODO 校验表是否存在，如果不存在，则创建Phoenix表
            if (Constants.SINK_TYPE_HBASE.equals(TableConfig.getSinkType().toLowerCase())) {
                checkTable(TableConfig.getSinkTable(),
                        TableConfig.getSinkColumns(),
                        TableConfig.getSinkPk(),
                        TableConfig.getSinkExtend());
            }

            // TODO 将数据写入状态广播处理
            BroadcastState<String, TableConfig> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

            // 拼接pk
            String pk = TableConfig.getSourceTable() + "-" + TableConfig.getOperateType();

            broadcastState.put(pk, TableConfig);

        } else {
            logger.warn("config table is empty pls check");
        }
    }

    /**
     * 处理主流数据
     *
     * @param value 数据
     * @param ctx   上下文
     * @param out   输出
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 提取状态数据
        ReadOnlyBroadcastState<String, TableConfig> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 获取key
        String key = value.getString("table") + "-" + value.getString("type");
        // 获取数据
        TableConfig tableConfig = broadcastState.get(key);

        // tableProcess 有可能为null,数据里面可能匹配不上表名和操作类型的key
        if (tableConfig != null) {
            // 根据配置信息过滤字段
            String sinkColumns = tableConfig.getSinkColumns();
            JSONObject data = value.getJSONObject("data");

            filterColumn(data, sinkColumns);
            String sinkType = tableConfig.getSinkType();
            // 将待写入的维表或者事实表添加至数据中，方便后续操作
            value.put("sinkTable", tableConfig.getSinkTable());

            // 将数据分流，Hbase数据写入侧输出流，Kafka数据写入主流
            if (Constants.SINK_TYPE_HBASE.equals(sinkType)) {
                ctx.output(outputTag, value);
            } else if (Constants.SINK_TYPE_KAFKA.equals(sinkType)) {
                out.collect(value);
            }
        } else {
            System.out.println("配置表不存在该key " + key);
            logger.warn("configuration don't exist the key" + key);
        }
    }

    /**
     * 根据配置信息过滤字段
     *
     * @param data        待过滤的数据
     * @param sinkColumns 目标字段
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        // TODO 处理目标字段
        System.out.println("sinkColumns>>>>>>>" + sinkColumns);
        String[] columns = sinkColumns.toLowerCase().split(",");
        // 数组没有contains方法，集合才有
        List<String> columnsList = Arrays.asList(columns);

        // TODO 遍历data数据，检查是否有目标字段，不包含就移除
        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnsList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }
        entries.removeIf(next -> !columnsList.contains(next.getKey()));
    }

    /**
     * 检验表是否存在
     * <p>
     * create table if not exist xx.xx (pk varchar primary key,name varchar ...) xxx;
     *
     * @param sinkTable   表名
     * @param sinkColumns 字段
     * @param sinkPk      主键
     * @param sinkExtend  扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {
        System.out.println("开始检验表");
        // TODO 处理主键及扩展字段
        // 字段判空
        if (sinkPk == null || sinkPk.equals("")) {
            sinkPk = "pk";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        // TODO 创建建表sql
        StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                .append(Constants.HBASE_SCHEME)
                .append(".")
                .append(sinkTable.toUpperCase())
                .append("(");
        // TODO 拆开建表字段
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            // 如果当前字段为主键
            if (sinkPk.equals(column)) {
                createTableSql
                        .append(column)
                        .append(" varchar ")
                        .append("primary key");
            } else {
                createTableSql.append(column).append(" varchar");
            }
            // 如果当前字段不是最后一个字段
            if (i < columns.length - 1) {
                createTableSql.append(",");
            }
        }
        // 拼接扩展字段
        createTableSql
                .append(")")
                .append(sinkExtend);

        String sql = createTableSql.toString();
        logger.info("create sql display" + sql);

        System.out.println(sql);

        PreparedStatement preparedStatement = null;

        // 执行sql建表
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        } catch (SQLException e) {
            System.out.println("建表失败" + e.getMessage());
            logger.error("create phoenix table failure：" + e.getMessage());
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
