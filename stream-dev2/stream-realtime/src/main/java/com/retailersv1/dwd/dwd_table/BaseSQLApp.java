package com.retailersv1.dwd.dwd_table;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
  @ClassDescription:
  @Author:fangyu_ren
  @Create:2024/12/27 S{TIME}
**/

public class BaseSQLApp {
    private static final String CDH_KAFAK_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String CDH_KAFKA_DB = ConfigUtils.getString("kafka.topic.db");
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");

    public static void main(String[] args) throws Exception {//StreamTableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
//读取 kafka业务数据:3> {"before":null,"after":{"id":7282,"log":"{\"common\":{\"ar\":\"29\",\"ba\":\"Redmi\",\"ch\":\"oppo\",\"is_new\":\"1\",\"md\":\"Redmi k50\",\"mid\":\"mid_100\",\"os\":\"Android 12.0\",\"sid\":\"3970b957-0d62-42ad-b5bf-02b972d52543\",\"vc\":\"v2.1.134\"},\"start\":{\"entry\":\"icon\",\"loading_time\":1169,\"open_ad_id\":14,\"open_ad_ms\":3298,\"open_ad_skip_ms\":59583},\"ts\":1731327462628}"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall","sequence":null,"table":"z_log","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1735181202413,"transaction":null}
//        读取 kafka业务数据:
        tEnv.executeSql("CREATE TABLE topic_db (\n" +
                "    `before` STRING,\n" +
                "    `after` STRING,\n" +
                "    `source` STRING,\n" +
                "    `op` STRING,\n" +
                "    `ts_ms` BIGINT,\n" +
                "    `transaction` String,\n" +
                "    `event_time` AS TO_TIMESTAMP_LTZ(ts_ms, 3),  -- 通过计算列将ts_ms转换为带时区的时间戳类型并定义为event_time字段\n" +
                "    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND  -- 基于event_time字段设置水位线，延迟5秒，可根据实际调整\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '"+CDH_KAFKA_DB+"',\n" +
                "    'properties.bootstrap.servers' = '"+CDH_KAFAK_SERVER+"',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");
        Table table = tEnv.sqlQuery("select * from topic_db");
        tEnv.toDataStream(table).print();
//        tEnv.executeSql("SELECT after[`name`] FROM topic_db").print();
//        before        after                             source                       op     ts_ms           transaction     event_time
//| +I |  <NULL> | {"id":499,"name":"短外套","... | {"version":"1.9.7.Final","c... |  r |  1735288789530 |   <NULL> | 2024-12-27 16:39:49.530 |
//| +I |  <NULL> | {"id":500,"name":"风衣","ca... | {"version":"1.9.7.Final","c... |  r |  1735288789530 |   <NULL> | 2024-12-27 16:39:49.530 |
//| +I |  <NULL> | {"id":497,"name":"正装裤","... | {"version":"1.9.7.Final","c... |  r |  1735288789529 |   <NULL> | 2024-12-27 16:39:49.529 |
//| +I |  <NULL> | {"id":587,"name":"毛线手套"... | {"version":"1.9.7.Final","c... |  r |  1735288789533 |   <NULL> | 2024-12-27 16:39:49.533 |
//| +I |  <NULL> | {"id":588,"name":"防晒手套"... | {"version":"1.9.7.Final","c... |  r |  1735288789533 |   <NULL> | 2024-12-27 16:39:49.533 |
//    tEnv.executeSql("select after from topic_db").print();
        //读取HBase的base_dic字典表
        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                " rowkey STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'gmall:dim_base_dic',\n" +
                " 'zookeeper.quorum' = '"+ CDH_ZOOKEEPER_SERVER+"'\n" +
                ")");
        tEnv.executeSql("select * from base_dic").print();








//       创建2.读取hbase数据




        env.execute();
    }








}