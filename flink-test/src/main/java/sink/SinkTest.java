package sink;

import bean.PhoneDetail;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author lihaoran
 */
public class SinkTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("/Users/lihaoran/Documents/flink-projects/flink-test/src/main/resources/phone.txt");
        SingleOutputStreamOperator<PhoneDetail> phoneStream = dataStreamSource.map(new MapFunction<String, PhoneDetail>() {
            @Override
            public PhoneDetail map(String s) throws Exception {
                String[] phone = s.split(",");
                String phoneName = phone[0];
                Double price = new Double(phone[1]);
                return new PhoneDetail(phoneName, price);
            }
        });
        phoneStream.addSink(new JDBCFunction());

        env.execute();
    }

    private static class JDBCFunction extends RichSinkFunction<PhoneDetail> {
        Connection connection = null;
        //生成连接和预编译语句
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306","root","123456");
            connection.prepareStatement("insert into table (phoneName,price) values(?,?) ");
            connection.prepareStatement("update table set price = ? where phoneName = ?");
        }

        /**
         * //每来一条数据执行一条sql
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(PhoneDetail value, Context context) throws Exception {
            //直接执行更新语句,如果没有更新执行插入
            updateStmt.setDouble(1,value.getPrice());
            updateStmt.setString(2,value.getPhoneName());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0){
                insertStmt.setString(1, value.getPhoneName());
                insertStmt.setDouble(2,value.getPrice());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
