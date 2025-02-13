package com.retailersv1.dwd.dwd_table;

import com.stream.common.utils.ConfigUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
/**
  @ClassDescription:
  @Author:fangyu_ren
  @Create:2024/12/27 S{TIME}
  11.3互动域评论事务事实表
**/

public class BaseSQLApp {
    private static final String CDH_KAFAK_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String CDH_KAFKA_DB = ConfigUtils.getString("kafka.topic.db");
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String white_kafka ="dwd_interaction_comment_info";
    private static final String Kafka_sql ="CREATE TABLE topic_db (\n" +
            "  `after` map<string,string>,\n" +
            "  `source` map<string,string>,\n" +
            "  `op` string ,\n" +
            "   `ts_ms` bigint,\n" +
            "  times AS TO_TIMESTAMP(FROM_UNIXTIME(ts_ms/1000))," +
            " WATERMARK FOR times AS times - INTERVAL '5' SECOND " +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = '"+CDH_KAFKA_DB+"',\n" +
            "  'properties.bootstrap.servers' = '"+CDH_KAFAK_SERVER+"',\n" +
            "  'properties.group.id' = 'testGroup',\n" +
            "  'scan.startup.mode' = 'earliest-offset',\n" +
            "  'format' = 'json'\n" +
            ")";
    private static final String Hbase_sql ="CREATE  TABLE base_dic (\n" +
            "    rowkey string,\n" +
            "    info ROW<dic_name string,parent_code string>,\n" +
            "    PRIMARY KEY (rowkey) NOT ENFORCED\n" +
            ") WITH (\n" +
            "    'connector' = 'hbase-2.2',\n" +
            "    'table-name' = 'gmall:dim_base_dic',\n" +
            "    'zookeeper.quorum' = '"+CDH_ZOOKEEPER_SERVER+"'\n" +
            ")";
    private static final String write_kafka_sql ="create table dwd_interaction_comment_info(" +
            "id string, " +
            "user_id string," +
            "sku_id string," +
            "appraise string," +
            "appraise_name string," +
            "comment_txt string," +
            "create_time bigint " +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = '"+white_kafka+"',\n" +
            "  'properties.bootstrap.servers' = '"+CDH_KAFAK_SERVER+"',\n" +
            "  'properties.group.id' = 'testGroup',\n" +
            "  'scan.startup.mode' = 'earliest-offset',\n" +
            "  'format' = 'json'\n" +
            ")";
    private static final String select_kafka=" select " +
            "  after['id'] id             ," +
            "  after['user_id'] user_id   ," +
            "  after['nick_name'] nick_name     ," +
            "  after['sku_id'] sku_id     ," +
            "  after['spu_id'] spu_id     ," +
            "  after['order_id'] order_id,\n" +
            "  after['appraise'] appraise,\n" +
            "  after['comment_txt'] comment_txt,\n" +
            "   times,\n"+
            "  TO_TIMESTAMP(FROM_UNIXTIME(CAST(after['create_time'] AS BIGINT)/1000))create_time  " +
            "  from topic_db where  " +
            "  source['table']='comment_info' ";
    private static final String DWD_pj_sql ="SELECT \n" +
            " c.id,\n" +
            " c.user_id,\n" +
            " c.sku_id,\n" +
            " c.spu_id,\n" +
            " c.order_id,\n" +
            " c.appraise,\n" +
            " c.comment_txt,\n" +
            " b.rowkey,\n" +
            " b.dic_name,\n" +
            " b.parent_code,\n" +
            " c.create_time\n" +
            "from comment_info as c\n" +
            "join base_dic for system_time as of c.proc_time as b\n" +
            "  on c.appraise = b.rowkey";
    public static void main(String[] args) throws Exception {//StreamTableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //读取kafka的数据封装为flinksql表------------读取kafka业务表
        System.out.println("读取kafka的数据封装为flinksql表");
        tEnv.executeSql(Kafka_sql).print();
        System.setProperty("HADOOP_USER_NAME","root");
        String hiveConfDir = "E://git仓库//my-master//stream-dev2//stream-realtime//src//main//resources";
        HiveCatalog hiveCatalog =new  HiveCatalog("hive-catalog","default",hiveConfDir);
        tEnv.registerCatalog("hive-catalog",hiveCatalog);
        tEnv.useCatalog("hive-catalog");
        //读取hbase封装为flinksql
        System.out.println("读取hbase封装为flinksql表");
        tEnv.executeSql(Hbase_sql).print();
        //读取kafka的评论数据
        Table pinglun =tEnv.sqlQuery(select_kafka);

//        pinglun.printSchema();
//        pinglun.execute().print();
        tEnv.createTemporaryView("comment_info",pinglun);
        System.out.println("读取kafka的评论数据");
        //读取HBase的base_dic字典表
        System.out.println("读取HBase的base_dic字典表-------------------------------");

        // 4. 事实表与维度表的 join: 拼接
        Table table = tEnv.sqlQuery(DWD_pj_sql);
        //5.根据关联数据创建表5. 通过 ddl 方式建表: 与 kafka 的 topic 管理 (sink) 、、
        tEnv.executeSql(write_kafka_sql).print();
//        6.将关联的数据写入到kafka
        table.insertInto(white_kafka).execute();
    }
}