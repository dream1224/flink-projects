package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PhoenixUtils {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixUtils.class);

    public static void executeSql(Connection connection, String sql) throws SQLException {
        PreparedStatement preparedStatement = null;

        try {
            // 预编译sql
            connection.prepareStatement(sql);
            // 执行写入数据操作 维度表数据量小可以不用批处理(通过数量或者时间)，历史数据初始化
//        preparedStatement.addBatch();
            preparedStatement.execute();
        } catch (SQLException e) {
            logger.error("execute sql failure：" + e.getMessage());
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

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
}
