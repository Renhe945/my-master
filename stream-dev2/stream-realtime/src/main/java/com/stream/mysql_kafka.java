package com.stream;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils_mysql;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
  @ClassDescription:
  @Author:fangyu_ren
  @Create:2024/12/26 S{TIME}
**/
public class mysql_kafka {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_topic_base_db = ConfigUtils.getString("kafka.topic.db");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //使用MySQL将数据读取来
        MySqlSource<String> mysqlSource = CdcSourceUtils_mysql.getMySQLCdcSource(//定义了一个mysql的读取
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        //在发送到kafka
        DataStreamSource<String> mysql_source = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql_Source");
        mysql_source.print("读取mysql的数据");
//        mysql_source.sinkTo(FlinkSinkUtil.getKafkaSink("topic.db"));//发送到kafkaConfigUtils.getString("kafka.topic.db"))

        mysql_source.sinkTo(KafkaUtils.buildKafkaSink( kafka_botstrap_servers,kafka_topic_base_db))
                .uid("sk_displayMsg2Kafka")
                .name("sk_displayMsg2Kafka");
        env.execute();
    }
}