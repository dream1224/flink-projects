import common.Constants;

import java.sql.*;

public class Test {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        try {

            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

            //这里配置zookeeper的地址，可单个，多个(用","分隔)可以是域名或者ip

            String url = "jdbc:phoenix:cluster003,cluster001,cluster002:2181";

            Connection conn = DriverManager.getConnection(url);

            Statement statement = conn.createStatement();

            long time = System.currentTimeMillis();

            ResultSet rs = statement.executeQuery("select * from FLINK_REALTIME.TEST");

            while (rs.next()) {
                String myKey = rs.getString("ID");
                String myColumn = rs.getString("NAME");
                System.out.println("ID=" + myKey + "NAME=" + myColumn);
            }

            long timeUsed = System.currentTimeMillis() - time;

            System.out.println("time " + timeUsed + "mm");

            // 关闭连接
            rs.close();
            statement.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
