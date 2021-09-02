package udtf;

import bean.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import udf.UDFTest;

public class UDTFTest {
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

        // 自定义表函数，将id拆分并输出
        Split split = new Split("_");

        // 需要在环境中注册udf
        tableEnvironment.createTemporarySystemFunction("split", split);
        // tableApi
        Table resultTable = userTable.joinLateral("split(name) as (word,length)").select("name,temperature,word,length");
        //flinkSql
        tableEnvironment.createTemporaryView("flinkUserTable",userStream);
        Table flinkUserTable = tableEnvironment.sqlQuery("select name,temperature,word,length from flinkUserTable,lateral table(split(name)) as splitid(word,length)");
        tableEnvironment.toAppendStream(resultTable, Row.class).print("tableDataStream");
//        tableEnvironment.toAppendStream(flinkUserTable, Row.class).print("flinkDataStream");

        environment.execute();
    }


    public static class Split extends TableFunction<Tuple2<String,Integer>> {
        private String cutter;

        public Split(String cutter) {
            this.cutter = cutter;
        }
        //必须实现eval方法，没有返回值
        public void eval(String str){
            for (String s : str.split(cutter)) {
                collect(new Tuple2<>(s,s.length()));
            }
        }
    }
}
