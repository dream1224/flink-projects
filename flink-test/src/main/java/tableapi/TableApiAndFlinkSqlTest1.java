package tableapi;

import bean.User;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.io.StreamTokenizer;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author lihaoran
 */
public class TableApiAndFlinkSqlTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> Stream = env.readTextFile("/Users/lihaoran/Documents/flink-projects/flink-test/src/main/resources/User.txt");

        SingleOutputStreamOperator<User> userStream = Stream.map(line -> {
            String[] split = line.split(",");
            return new User(split[0], new Double(split[1]), new Long(split[2]));
        });

        //  创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        调用tableApi
        Table table = tableEnv.fromDataStream(userStream);
        Table user = table.select($("name"), $("temperature"), $("time"));
        DataStream<Row> userNewStream = tableEnv.toAppendStream(user, Row.class);
//        userNewStream.print("tableApi");

        // 使用flinkSql
        tableEnv.createTemporaryView("userTable", userStream);
        Table userTable2 = tableEnv.sqlQuery("select * from  userTable");
        DataStream<Row> userStream2 = tableEnv.toAppendStream(userTable2, Row.class);
//        userStream2.print("flinkSql");


        // 获取schema及其操作
//        System.out.println(userTable2.getSchema());
//        for (String fieldName : userTable2.getSchema().getFieldNames()) {
//            System.out.println(fieldName);
//        }
//        userTable2.printSchema();

        // TableResult
//        TableResult tableResult = tableEnv.executeSql("select * from  userTable");
//        tableResult.print();



        // ResultKind defines the types of the result.  SUCCESS or SUCCESS_WITH_CONTENT
//        ResultKind resultKind = tableResult.getResultKind();
//        System.out.println(resultKind);

        //获取schema及数据
//        TableSchema tableSchema = tableResult.getTableSchema();
        //获取列名及其类型
//        for (String fieldName : tableSchema.getFieldNames()) {
//            System.out.println("列名：" + fieldName + "; 类型：" + tableSchema.getFieldDataType(fieldName));
//        }
        //获取列数
//        int fieldCount = tableSchema.getFieldCount();
//        System.out.println(fieldCount);
        // 将流转换为table proctime获取处理时间 rowtime事件时间
        Table users = tableEnv.fromDataStream(userStream,"name,temperature as temp,time as ts,p.proctime");
        tableEnv.createTemporaryView("tempUser",users);

        user.printSchema();
        // 开窗
//        users.window(Tumble.over("5.seconds").on("ts").as("w"))
//                .groupBy("name")
//                .select("name,name.count,temp.avg,w.end");

//        tableEnv.sqlQuery("select name,count(name) as cnt,tumble_end(ts,interval '10' second) from tempUser group by name,tumble(ts,interval '10' second)");
        // 将表转换为流
        tableEnv.toAppendStream(users,Row.class).print();

        env.execute();


    }
}
