package common.conn;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

// ORM 对象关系映射
public class PhoenixUtils {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixUtils.class);

    public static void executeSql(Connection connection, String sql) throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            // 预编译sql
            preparedStatement = connection.prepareStatement(sql);
            // 执行写入数据操作 维度表数据量小可以不用批处理(通过数量或者时间)，历史数据初始化
//        preparedStatement.addBatch();
            preparedStatement.execute();
            connection.commit();
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

    /**
     * @param sql
     * @param clz
     * @param underScoreToCamel 转换
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) {
        PreparedStatement preparedStatement = null;
        ArrayList<T> list = new ArrayList<>();
        try {
            // 预编译Sql
            preparedStatement = connection.prepareStatement(sql);

            // 执行查询
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 解析查询结果ResultSet，封装成T对象
            while (resultSet.next()) {
                // 构建泛型对象
                T t = clz.newInstance();

                for (int i = 1; i < columnCount + 1; i++) {
                    // 取出列名
                    String columnName = metaData.getColumnName(i);
                    // 下划线转驼峰命名
                    if (underScoreToCamel) {
                        // Guava包下的工具类
                        columnName = CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL).convert(columnName);
                    }
                    // 取出数值
                    Object value = resultSet.getObject(i);

                    // 将值设置给对象
                    // commons-beanutils
                    BeanUtils.setProperty(t, columnName, value);
                }
                // 将当前对象添加至集合
                list.add(t);

            }


            // 返回数据


        } catch (Exception e) {
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
        return list;
    }
}
