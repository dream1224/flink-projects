package udf;

import bean.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;


public class UDFTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        environment.setParallelism(1);
        DataStreamSource<String> dataStreamSource = environment.readTextFile("/Users/lihaoran/Documents/flink-projects/flink-test/src/main/resources/User.txt");
        DataStream<User> userStream = dataStreamSource.map(new MapFunction<String, User>() {
            @Override
            public User map(String value) throws Exception {
                String[] split = value.split(",");
                return new User(split[0], new Double(split[1]), new Long(split[2]));
            }
        });

//        userStream.print("userStream");

        Table userTable = tableEnvironment.fromDataStream(userStream, "name,temperature,time");

        // 自定义标量函数，求hash值
        HashCode hashCode = new HashCode(10);
        // 需要在环境中注册udf
        tableEnvironment.registerFunction("hashCode", hashCode);
        // tableApi
        Table resultTable = userTable.select("name,temperature,hashCode(name)");
        //flinkSql
        tableEnvironment.createTemporaryView("flinkUserTable",userStream);
        Table flinkUserTable = tableEnvironment.sqlQuery("select name,temperature,hashCode(name) from flinkUserTable");
//        tableEnvironment.toAppendStream(resultTable, Row.class).print("tableDataStream");
        tableEnvironment.toAppendStream(flinkUserTable, Row.class).print("flinkDataStream");

        environment.execute();
    }

    public static class HashCode extends ScalarFunction {

        private int factor;

        public HashCode(int factor) {
            this.factor = factor;
        }
        //必须实现eval方法
        public int eval(String str) {
            return str.hashCode() * factor;
        }
    }
}

