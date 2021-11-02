package common.process;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
    /**
     * 将维度信息补充到事实数据上
     *
     * @param input
     * @param dimInfo
     */
    void joinDim(T input, JSONObject dimInfo);

    /**
     * 根据数据获取主键
     *
     * @param input
     * @return
     */
    String getPk(T input);
}
