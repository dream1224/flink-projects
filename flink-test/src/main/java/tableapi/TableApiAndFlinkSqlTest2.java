package tableapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class TableApiAndFlinkSqlTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.12默认使用blinkplaner
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        使用老版本的planner,构造方法为private,用builder构造
//        EnvironmentSettings oldBuild = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
//        StreamTableEnvironment oldTEnv = StreamTableEnvironment.create(env, oldBuild);

        tEnv.connect(new FileSystem().path("/Users/lihaoran/Documents/flink-projects/flink-test/src/main/resources/User.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("name", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE())
                        .field("ts", DataTypes.BIGINT())
                ).createTemporaryTable("inputTable");
        Table inputTable = tEnv.from("inputTable");

        tEnv.connect(new FileSystem().path("/Users/lihaoran/Documents/flink-projects/flink-test/src/main/resources/out.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("name", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE())
                        .field("ts", DataTypes.BIGINT())
                ).createTemporaryTable("outputTable");
        Table outputTable = tEnv.from("outputTable");
        inputTable.insertInto("outputTable");
//        inputTable.printSchema();
        //有更新用toRetractStream

        //写入外部时更新模式 追加append 撤回retract(先删后插) 更新插入upsert(upsert和delete)
        tEnv.toAppendStream(inputTable, Row.class).print();
        env.execute();
    }
}
