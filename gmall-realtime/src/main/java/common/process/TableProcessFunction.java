package common.process;

import bean.TableConfig;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.DataUtils;
import utils.PhoenixUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * * @param <IN1> The input type of the non-broadcast side. 非广播流输入
 * * @param <IN2> The input type of the broadcast side. 广播流输入
 * * @param <OUT> The output type of the operator. 输出类型
 */
@SuppressWarnings("Duplicates")
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    // 定义日志打印器
    private static final Logger logger = LoggerFactory.getLogger(TableProcessFunction.class);
    // 定义phoenix连接
    private Connection connection;
    // 定义OutputTag，侧输出流标签
    private OutputTag<JSONObject> outputTag;
    // 定义Map状态描述器
    private MapStateDescriptor<String, TableConfig> mapStateDescriptor;

    public TableProcessFunction() {
    }

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableConfig> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    /**
     * 获取Phoenix连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(Constants.PHOENIX_DRIVER);
        try {
            connection = DriverManager.getConnection(Constants.PHOENIX_SERVER);
        } catch (SQLException e) {
            logger.warn("get phoenix connection failure,reset connection");
            e.printStackTrace();
            this.open(parameters);
        }
    }

    /**
     * 关闭Phoenix连接
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }


    /**
     * 处理进来的广播数据(配置表)
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
            // TODO 将数据写入状态广播处理
            BroadcastState<String, TableConfig> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

            // 拼接pk
            String broadcastPk = TableConfig.getSourceTable() + "-" + TableConfig.getOperateType();

            String operation = jsonObject.getString("type");

            if (!"delete".equals(operation)) {
                // TODO 如果配置表添加或修改数据，则先校验Phoenix表是否存在，如果不存在，则创建Phoenix表
                if (Constants.SINK_TYPE_HBASE.equals(TableConfig.getSinkType().toLowerCase())) {
                    checkTable(TableConfig.getSinkTable(),
                            TableConfig.getSinkColumns(),
                            TableConfig.getSinkPk(),
                            TableConfig.getSinkExtend());
                }
                // TODO 将这条数据放入BroadcastState
                broadcastState.put(broadcastPk, TableConfig);

            } else {
                // TODO 如果配置表删除一条数据,则从广播状态中移除该条数据
                broadcastState.remove(broadcastPk);
                logger.info("BroadcastState remove this pk>>>>>>" + broadcastPk);
                System.out.println("移除广播pk>>>>>> " + broadcastPk);

                // 创建存储表名的List
                List<String> tableList = new ArrayList<>();
                // TODO 获取BroadcastState存储中所有的表名放入List
                Iterator<Map.Entry<String, bean.TableConfig>> iterator = broadcastState.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, bean.TableConfig> next = iterator.next();
                    tableList.add(next.getKey().split("-")[0]);
                }

                if (!tableList.contains(TableConfig.getSourceTable())) {
                    // TODO 如果配置表删除一张表的所有数据则删除这张表
                    dropTable(TableConfig.getSinkTable());
                    logger.warn("table " + TableConfig.getSinkTable() + " is dropped");
                    System.out.println("Phoenix表" + TableConfig.getSinkTable() + "已删除");
                }
            }
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

        // tableProcess有可能为null,数据里面可能匹配不上表名和操作类型的key
        if (tableConfig != null) {
            // 根据配置信息过滤字段
            String sinkColumns = tableConfig.getSinkColumns();
            JSONObject data = value.getJSONObject("data");
            filterColumn(data, sinkColumns);

            // 将待写入的表(维表或者事实表),以及操作类型添加至数据中，方便后续操作
            String sinkType = tableConfig.getSinkType();
            value.put("sinkTable", tableConfig.getSinkTable());
            value.put("type", tableConfig.getOperateType());
            // 获取pk要拼接的字段
            String sinkPk = tableConfig.getSinkPk();
            if (sinkPk == null || sinkPk.equals("")) {
                logger.error("sinkPk is not allowed empty");
                return;
            }
            String pkValues = DataUtils.makePk(value, sinkPk);
            // 将pk写入JSON
            value.put("pk", pkValues);

            // 将数据分流，Hbase数据写入侧输出流，Kafka数据写入主流
            if (Constants.SINK_TYPE_HBASE.equals(sinkType)) {
                ctx.output(outputTag, value);
            } else if (Constants.SINK_TYPE_KAFKA.equals(sinkType)) {
                out.collect(value);
            }
        } else {
            System.out.println("配置表不存在该key>>>>>> " + key);
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
        System.out.println("sinkColumns>>>>>> " + sinkColumns);
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
     * 删除表
     *
     * @param sinkTable
     */
    private void dropTable(String sinkTable) throws SQLException {
        // 创建删表sql
        StringBuilder makeDropTableSql = new StringBuilder();
        makeDropTableSql
                .append("drop table if exists ")
                .append(Constants.HBASE_SCHEME)
                .append(".")
                .append(sinkTable);
        String dropTableSql = makeDropTableSql.toString();
        System.out.println("dropTableSql>>>>>> " + dropTableSql);
        // 执行sql
        PhoenixUtils.executeSql(connection, dropTableSql);
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
            logger.error("sinkPk is not allowed empty");
            return;
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
            if ("pk".equals(column)) {
                createTableSql
                        .append(column)
                        .append(" varchar primary key");
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
        // 执行sql建表
        PhoenixUtils.executeSql(connection, sql);
    }
}
