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
        flink_kafka(tEnv);

//        env.execute();
    }
    private static void flink_kafka(StreamTableEnvironment tEnv) {
        //读取kafka的数据封装为flinksql表
        tEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `after` map<string,string>,\n" +
                "  `source` map<string,string>,\n" +
                "  `op` string ,\n" +
                "   `ts_ms` bigint,\n" +
                "  times AS TO_TIMESTAMP(FROM_UNIXTIME(ts_ms/1000))," +
                " WATERMARK FOR times AS times - INTERVAL '0' SECOND " +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+ CDH_KAFKA_DB +"',\n" +
                "  'properties.bootstrap.servers' = '"+ CDH_KAFAK_SERVER +"',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        tEnv.executeSql("select * from topic_db").print();
//        //读取kafka的数据
//        tEnv.sqlQuery(" select after['id'] id," +
//                "after['user_id'] user_id," +
//                "after['sku_id'] sku_id," +
//                "TO_TIMESTAMP(FROM_UNIXTIME(CAST(after['create_time'] AS BIGINT)/1000)) create_time "+
//                "from topic_db where " +
//                "source['table']='favor_info' " +
//                "").execute().print();
    }
    private static void flink_hbase(StreamTableEnvironment tEnv) {
        //读取HBase的base_dic字典表//创建2.读取hbase数据
        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                "    rowkey STRING,\n" +
                "    info ROW<dic_name STRING>,\n" +
                "    PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'hbase-2.2',\n" +
                "    'table-name' = 'gmall:dim_base_dic',\n" +
                "    'zookeeper.quorum' = '"+CDH_ZOOKEEPER_SERVER+"'\n" +
                ")");
//        tEnv.executeSql("select * from base_dic").print();

    }






}