package common.process;

import com.alibaba.fastjson.JSONObject;
import common.mapping.Constants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import common.conn.DimUtils;
import common.conn.ThreadPoolUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    // 声明线程池
    private ThreadPoolExecutor threadPoolExecutor;

    // 定义Phoenix连接
    private Connection connection;

    // 定义表名
    private String tableName;

    // 引入表名
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(Constants.PHOENIX_DRIVER);
        try {
            threadPoolExecutor = ThreadPoolUtils.getThreadPoolExecutor();
            connection = DriverManager.getConnection(Constants.PHOENIX_SERVER);
        } catch (SQLException e) {
            this.open(parameters);
            e.printStackTrace();
        }
    }

    /**
     * @Description
     * @Author Lhr
     * @Date 2021/11/2 10:49
     * @param: input
     * @param: resultFuture
     */
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {

            @Override
            public void run() {
                // 查询维度数据
                JSONObject dimInfo = DimUtils.getDimInfo(connection, tableName, getPk(input));

                // 补充维度信息
                joinDim(input, dimInfo);

                // 将数据写出
                resultFuture.complete(Collections.singleton(input));

            }
        });
    }


    /**
     * 如果超时，执行这个方法
     *
     * @param input
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut" + input);
        ;
    }

}
