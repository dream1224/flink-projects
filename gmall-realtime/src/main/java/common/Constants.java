package common;


public class Constants {
    // KAFKA
    public static final String BOOTSTRAP_SERVER = "cluster001:9092,cluster002:9092,cluster003:9092";
    public static final String DEFAULT_DWD_TOPIC = "dwd_default_topic";
    public static final String KAFKA_ODS_LOG_TOPIC = "ods_base_log";
    public static final String KAFKA_LOG_GROUP_ID = "log-group";
    public static final String KAFKA_DB_GROUP_ID = "db-group";
    public static final String KAFKA_ODS_DB_TOPIC = "ods_base_db";
    public static final String KAFKA_DWD_PAGE_TOPIC = "dwd_page_log";
    public static final String KAFKA_DWD_START_TOPIC = "dwd_start_log";
    public static final String KAFKA_DWD_DISPLAY_TOPIC = "dwd_display_log";
    public static final String KAFKA_DWD_ORDER_INFO_TOPIC = "dwd_order_info";
    public static final String KAFKA_DWD_ORDER_DETAIL_TOPIC = "dwd_order_detail";
    public static final String UV_GROUP_ID = "uv_group";
    public static final String KAFKA_DWM_UV_TOPIC = "dwm_uv";
    public static final String KAFKA_DWM_USER_JUMP_DETAIL_TOPIC = "dwm_user_jump_detail";
    public static final String USER_JUMP_DETAIL_GROUP_ID = "user_jump_detail_group";
    public static final String KAFKA_ORDER_WIDE = "dwm_order_wide";
    public static final String ORDER_WIDE_GROUP = "dwm_order_wide_group";

    // CK
    public static final long CHECKPOINT_TIME = 5000L;
    public static final long CHECKPOINT_OUT_TIME = 10000L;
    public static final String CHECKPOINT_URL = "hdfs://cluster001:8020/flink/ck";

    //动态分流Sink常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    //Phoenix库名
    public static final String HBASE_SCHEME = "FLINK_REALTIME";
    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:cluster001,cluster002,cluster003:2181";

    // Mysql
    public static final String USERNAME = "root";
    public static final String PASSWORD = "123456";
    public static final int PORT = 3306;
    public static final String HOSTNAME = "cluster001";

    // Redis
    public static final int MAX_TOTAL = 100;
    public static final int MAX_WAIT_MIllIS = 2000;
    public static final int MAX_IDLE = 5;
    public static final int MIN_IDLE = 5;
}
