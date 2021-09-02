package utils;

import common.Constants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CKUtils {
    public static void setCk(StreamExecutionEnvironment environment) {
        // 开启CK
        environment.enableCheckpointing(Constants.CHECKPOINT_TIME);
        // 设置checkpoint超时时间
        environment.getCheckpointConfig().setCheckpointTimeout(Constants.CHECKPOINT_OUT_TIME);
        // 设置checkpoint精准一次  mysql必须要设置autocommit=false 否则会报错
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // cancel任务时保存最后一次checkpoint
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 重启策略
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        // 状态后端
        environment.setStateBackend(new FsStateBackend(Constants.CHECKPOINT_URL));
        // 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "HDFS");
    }
}
