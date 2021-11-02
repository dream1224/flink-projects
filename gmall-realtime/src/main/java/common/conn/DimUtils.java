package common.conn;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.conn.PhoenixUtils;
import common.conn.RedisUtils;
import common.mapping.Constants;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtils {
    /**
     * 查询用户维度信息
     *
     * @param tableName 表名
     * @param pk        主键
     * @return
     */
    public static JSONObject getDimInfo(Connection connection, String tableName, String pk) {
        // 先查询Redis数据
        // 获取Redis连接
        Jedis jedis = RedisUtils.getJedis();
        // 拼接RedisKey
        String redisKey = tableName + "-" + pk;
        // 查询Redis数据
        String valueJson = jedis.get(redisKey);
        if (valueJson != null) {
            JSONObject jsonObject = JSON.parseObject(valueJson);
            // 保留一天的热点数据
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return jsonObject;
        }

        // 当Redis查询不到数据时
        // 封装查询SQL语句
        String querySql = "select * from " + Constants.HBASE_SCHEME + "." + tableName + "where pk ='" + pk + "'";

        // 查询Phoenix
        List<JSONObject> queryList = PhoenixUtils.queryList(connection, querySql, JSONObject.class, false);
        JSONObject jsonObject = queryList.get(0);

        // 将数据写入Redis
        jedis.set(redisKey, jsonObject.toJSONString());
        // 设置过期时间
        jedis.expire(redisKey, 24 * 60 * 60);
        // 归还jedis连接
        jedis.close();

        // 返回结果
        return queryList.get(0);
    }

    /**
     * 当维度数据更新时，需要先删除Redis中已缓存的的数据，不然查询Phoenix和Redis结果会不一致
     *
     * @param tableName
     * @param pk
     */
    public static void deleteRedis(String tableName, String pk) {
        // 获取连接
        Jedis jedis = RedisUtils.getJedis();

        // 拼接RedisKey
        String redisKey = tableName + "-" + pk;

        // 执行删除操作
        jedis.del(redisKey);

        // 释放连接
        jedis.close();
    }
}
