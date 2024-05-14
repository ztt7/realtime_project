package cn.doitedu.rtdw.etl;

import ch.hsr.geohash.GeoHash;
import com.alibaba.fastjson.JSON;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;

/**
 * 用户行为日志数据公共维度退维任务
 */
public class EtlJob01_UserEventsLogCommonDim {
    public static void main(String[] args) {
        // flink程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 一些必须参数设置
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/e:/test_doitedu");

        // 构造table编程环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 建kafka连接器的映射表，读取 kafka中的用户行为日志
        tenv.executeSql(
                " CREATE TABLE mall_events_kafka_source (               "
                        // 物理字段
                        + "     username     string,                            "
                        + "     session_id   string,                            "
                        + "     eventId      string,                            "
                        + "     eventTime    bigint,                            "
                        + "     lat          double,                            "
                        + "     lng          double,                            "
                        + "     release_channel   string,                       "
                        + "     device_type       string,                       "
                        + "     properties   map<string,string>,                "
                        // 表达式字段,声明了process time语义的时间戳字段，用于后续的lookup join
                        + "     proc_time   AS PROCTIME()                       "
                       /* + "   event_time    AS  to_timestamp_ltz(eventTime,3) "
                        + "     watermark for event_time as event_time - interval '5' second "*/
                        + " ) WITH (                                            "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'mall-events-log',                           "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092',   "
                        + "  'properties.group.id' = 'goo1',                   "
                        + "  'scan.startup.mode' = 'latest-offset',           "
                        + "  'value.format'='json',                             "
                        + "  'value.json.fail-on-missing-field'='false',        "
                        + "  'value.fields-include' = 'EXCEPT_KEY'              "
                        + " )                                                   ");

        tenv.executeSql("select * from mall_events_kafka_source").print();

        // 创建hbase连接器表： 用户注册信息维表
        tenv.executeSql(
                "CREATE TABLE ums_member_hbase_source ( " +
                        " username STRING,                              " +
                        " f ROW<id BIGINT,phone STRING, status INT, create_time TIMESTAMP(3), gender INT, birthday DATE, province STRING, city STRING, job STRING, source_type INT>, " +
                        " PRIMARY KEY (username) NOT ENFORCED           " +
                        ") WITH (                                       " +
                        " 'connector' = 'hbase-2.2',                    " +
                        " 'table-name' = 'dim_user_info',               " +
                        " 'zookeeper.quorum' = 'doitedu:2181'           " +
                        ")");

        // 创建hbase连接器表： geohash码地域维表
        tenv.executeSql(
                "CREATE TABLE dim_geo_area_hbase_source ( " +
                        " geohash STRING,                                      " +
                        " f ROW<province STRING, city STRING, region STRING>, " +
                        " PRIMARY KEY (geohash) NOT ENFORCED            " +
                        ") WITH (                                       " +
                        " 'connector' = 'hbase-2.2',                    " +
                        " 'table-name' = 'dim_geo_area',                " +
                        " 'zookeeper.quorum' = 'doitedu:2181'           " +
                        ")");
        // 创建hbase连接器表： 页面信息维表
        tenv.executeSql(
                " CREATE TABLE dim_page_info_hbase_source( " +
                        "   url_prefix  STRING,                    " +
                        "   f ROW<pt STRING,sv STRING>             " +
                        " ) WITH (                                 " +
                        "   'connector' = 'hbase-2.2',             " +
                        "   'table-name' = 'dim_page_info',        " +
                        "   'zookeeper.quorum' = 'doitedu:2181'    " +
                        " )                                        ");

        // 函数注册到元数据空间
        tenv.createTemporaryFunction("geo", GeoHashFunction.class);


        /**
         * 日志数据关联公共维度的join语句
         */
        // 支持延迟重试的lookup新特性（从flink-1.16.0开始才有的）
        tenv.executeSql(
                "CREATE TEMPORARY VIEW wide_view AS  " +
                        // flink-1.16.0增强的新特性：lookup查询支持延迟重试
                        "SELECT  /*+ LOOKUP('table'='ums_member_hbase_source', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='5s','max-attempts'='3') */" +
                        "u.f.id as user_id,e.username,e.session_id,e.eventId as event_id,e.eventTime as event_time,e.lat,e.lng," +
                        "e.release_channel,e.device_type,e.properties," +
                        "u.f.phone as register_phone,u.f.status as user_status,u.f.create_time as register_time," +
                        "u.f.gender as register_gender,u.f.birthday as register_birthday,u.f.province as register_province,u.f.city as register_city," +
                        "u.f.job as register_job,u.f.source_type as register_source_type," +
                        "g.f.province as gps_province ,g.f.city as gps_city,g.f.region as gps_region, " +
                        "p.f.pt as page_type, p.f.sv as page_service " +
                        "FROM mall_events_kafka_source AS e  " +
                        "LEFT JOIN ums_member_hbase_source FOR SYSTEM_TIME AS OF e.proc_time AS u  ON e.username = u.username " +
                        // geohash，使用sql函数REVERSE反转关联hbase中的rowkey（因为hbase中就是反转存储的，避免热点问题）
                        "LEFT JOIN dim_geo_area_hbase_source FOR SYSTEM_TIME AS OF e.proc_time AS g ON REVERSE(geo(e.lat,e.lng)) = g.geohash   " +
                        // regexp_extract 做正则抽取
                        "LEFT JOIN dim_page_info_hbase_source FOR SYSTEM_TIME AS OF e.proc_time AS p ON regexp_extract(e.properties['url'],'(^.*/).*?') = p.url_prefix "
        );

        tenv.executeSql("select * from wide_view").print();



        /**
         * 将关联结果写出到外部存储
         *   1. 到 kafka 的 dwd层topic
         *   2. 到 doris 的表
         */

        // 建kafka连接器表，映射目标输出的topic
        tenv.executeSql(
                " CREATE TABLE mall_events_wide_kafkasink(          "
                        + "     user_id           BIGINT,                         "
                        + "     username          string,                         "
                        + "     session_id        string,                         "
                        + "     event_Id          string,                         "
                        + "     event_time        bigint,                         "
                        + "     lat               double,                         "
                        + "     lng               double,                         "
                        + "     release_channel   string,                         "
                        + "     device_type       string,                         "
                        + "     properties        map<string,string>,             "
                        + "     register_phone    STRING,                         "
                        + "     user_status       INT,                            "
                        + "     register_time     TIMESTAMP(3),                   "
                        + "     register_gender   INT,                            "
                        + "     register_birthday DATE, register_province STRING, "
                        + "     register_city STRING, register_job STRING, register_source_type INT,   "
                        + "     gps_province   STRING, gps_city STRING, gps_region STRING,             "
                        + "     page_type   STRING, page_service STRING         "
                        + " ) WITH (                                            "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'dwd-events-log',                      "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092',   "
                        + "  'properties.group.id' = 'testGroup',               "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'value.format'='json',                             "
                        + "  'value.json.fail-on-missing-field'='false',        "
                        + "  'value.fields-include' = 'EXCEPT_KEY')             ");

        // 将关联好的宽表数据插入kafka
        tenv.executeSql("insert into mall_events_wide_kafkasink select * from wide_view");


        // 建doris连接器表，映射目标输出的doris表
        tenv.executeSql(
                " CREATE TABLE mall_events_wide_doris_sink(         "
                        + "     gps_province         VARCHAR(16),   "
                        + "     gps_city             VARCHAR(16),   "
                        + "     gps_region           VARCHAR(16),   "
                        + "     dt                   DATE,          "
                        + "     user_id              BIGINT,           "
                        + "     username             VARCHAR(20),   "
                        + "     session_id           VARCHAR(20),   "
                        + "     event_id             VARCHAR(10),   "
                        + "     event_time           bigint,        "
                        + "     lat                  DOUBLE,        "
                        + "     lng                  DOUBLE,        "
                        + "     release_channel      VARCHAR(20),   "
                        + "     device_type          VARCHAR(20),   "
                        + "     properties           VARCHAR(40),   "
                        + "     register_phone       VARCHAR(20),   "
                        + "     user_status          INT,           "
                        + "     register_time        TIMESTAMP(3),  "
                        + "     register_gender      INT,           "
                        + "     register_birthday    DATE,          "
                        + "     register_province    VARCHAR(20),   "
                        + "     register_city        VARCHAR(20),   "
                        + "     register_job         VARCHAR(20),   "
                        + "     register_source_type INT        ,   "
                        + "     page_type            VARCHAR(20),   "
                        + "     page_service         VARCHAR(20)    "
                        + " ) WITH (                               "
                        + "    'connector' = 'doris',              "
                        + "    'fenodes' = 'doitedu:8030',         "
                        + "    'table.identifier' = 'dwd.mall_events_wide',  "
                        + "    'username' = 'root',                "
                        + "    'password' = '',                    "
                        + "    'sink.label-prefix' = 'doris_label" + System.currentTimeMillis() + "'"
                        + " )                                         "
        );
        // 将关联好的宽表数据插入doris
        tenv.createTemporaryFunction("toJson", HashMap2Json.class);
        tenv.executeSql("INSERT INTO mall_events_wide_doris_sink                                      "
                + " SELECT                                                                         "
                + "     gps_province         ,                                                     "
                + "     gps_city             ,                                                     "
                + "     gps_region           ,                                                     "
                + "     TO_DATE(DATE_FORMAT(TO_TIMESTAMP_LTZ(event_time, 3),'yyyy-MM-dd')) as dt,  "
                + "     user_id              ,                                                     "
                + "     username             ,                                                     "
                + "     session_id           ,                                                     "
                + "     event_id             ,                                                     "
                + "     event_time           ,                                                     "
                + "     lat                  ,                                                     "
                + "     lng                  ,                                                     "
                + "     release_channel      ,                                                     "
                + "     device_type          ,                                                     "
                + "     toJson(properties) as properties     ,                                     "
                + "     register_phone       ,                                                     "
                + "     user_status          ,                                                     "
                + "     register_time        ,                                                     "
                + "     register_gender      ,                                                     "
                + "     register_birthday    ,                                                     "
                + "     register_province    ,                                                     "
                + "     register_city        ,                                                     "
                + "     register_job         ,                                                     "
                + "     register_source_type ,                                                     "
                + "     page_type            ,                                                     "
                + "     page_service                                                               "
                + " FROM   wide_view                                                               "
        );
    }


    public static class HashMap2Json extends ScalarFunction {
        public String eval(Map<String,String> properties){
            return JSON.toJSONString(properties);
        }
    }

    public static class GeoHashFunction extends ScalarFunction{

        public String eval(Double lat,Double lng){
            String s = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5);
            return s;
        }


    }
}
