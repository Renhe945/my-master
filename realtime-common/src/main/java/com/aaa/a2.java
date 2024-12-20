package com.aaa;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.utils.ConfigUtils;

public class a2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        MySqlSource<String> myS0LDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
        ConfigUtils.getString("mysql.database"),
        " ",
        ConfigUtils.getString("mysql.user"),
        ConfigUtils.getstring("mysql.pwd"),
        Startup0ptions.latest()
        );

        cdcDbMainStream.sinkTo(
                KafkaUtils.buildKafkasink(ConfigUtils.getstring( "kafka.bootstrap.servers"), "realtime_v1_mysql_db")
        ).uid("sink_to_kafka_realtime_v1_mysql_db")
        .name("sink_to_kafka_realtime_v1_mysql_db");

        env.execute();
    }
}
