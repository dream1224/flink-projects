package utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.Constants;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DimUtils {
    /**
     * @param tableName 表名
     * @param pk        主键
     * @return
     */
    public static JSONObject getDimInfo(Connection connection, String tableName, String pk) {
        // 先查询Redis数据
        Jedis jedis = RedisUtils.getJedis();
        String redisKey = tableName + "-" + pk;
        String valueJson = jedis.get(redisKey);
        if (valueJson != null) {
            JSONObject jsonObject = JSON.parseObject(valueJson);
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return jsonObject;
        }

        // 当Redis查询不到数据时
        // 封装SQL语句
        String querySql = "select * from " + Constants.HBASE_SCHEME + "." + tableName + "where pk ='" + pk + "'";

        // 查询Phoenix
        List<JSONObject> queryList = PhoenixUtils.queryList(connection, querySql, JSONObject.class, false);
        JSONObject jsonObject = queryList.get(0);

        // 将数据写入Redis
        jedis.set(redisKey, jsonObject.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        // 返回结果
        return queryList.get(0);
    }
}
