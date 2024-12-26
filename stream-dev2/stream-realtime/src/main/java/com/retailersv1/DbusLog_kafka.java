package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDim;
import com.stream.FlinkSinkUtil;
import com.stream.common.utils.ConfigUtils;
import com.stream.utils.CdcSourceUtils_mysql;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
  @ClassDescription:
  @Author:fangyu_ren
  @Create:2024/12/25
**/
public class DbusLog_kafka {
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //使用MySQL将数据读取来
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .databaseList(ConfigUtils.getString("mysql.database")) // monitor all tables under inventory database
                .username(ConfigUtils.getString("mysql.user"))
                .password(ConfigUtils.getString("mysql.pwd"))
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();
        //在发送到kafka
        DataStreamSource<String> mysql_source = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql_Source");
        mysql_source.print("读取mysql的数据");
        mysql_source.sinkTo(FlinkSinkUtil.getKafkaSink("topic_db"));//发送到kafka
        KafkaSource<String> build = KafkaSource.<String>builder()//读取kafka的数据
                .setBootstrapServers(ConfigUtils.getString("kafka.bootstrap.servers"))
                .setTopics(ConfigUtils.getString("kafka.topic.db"))
                .setGroupId("groupId")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null) {
                            return new String(bytes, StandardCharsets.UTF_8);
                        }
                        return null;
                    }
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                }).build();
        DataStreamSource<String> kafka_source = env.fromSource(build, WatermarkStrategy.noWatermarks(), "kafka_Source");
        kafka_source.print("读取 kafka 业务数据");
        //使用kafka业务数据分流
        MySqlSource<String> mysql_pz = CdcSourceUtils_mysql.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "gmall2023_config.table_process_dim",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        DataStreamSource<String> pz = env.fromSource(mysql_pz, WatermarkStrategy.noWatermarks(), "mysql_pz");
        pz.print("配置表数据");
        SingleOutputStreamOperator<JSONObject> KafkaMainStreamMap = kafka_source.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);

        SingleOutputStreamOperator<JSONObject> pzDimStreamMap = pz.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                .setParallelism(1);


       SingleOutputStreamOperator<JSONObject> StreamMapCleanColumn = pzDimStreamMap.map(s -> {
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d".equals(s.getString("op"))){//查询是否为删除标识“d"
                        resJson.put("before",s.getJSONObject("before"));
                    }else {
                        resJson.put("after",s.getJSONObject("after"));
                    }
                    resJson.put("op",s.getString("op"));
                    return resJson;
                }).uid("clean_json_column_map")
                .name("clean_json_column_map");

       SingleOutputStreamOperator<JSONObject> tpDS = StreamMapCleanColumn.map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER, CDH_HBASE_NAME_SPACE))
               .uid("map_create_hbase_dim_table")
               .name("map_create_hbase_dim_table");
       MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
       BroadcastStream<JSONObject> broadcastDs = tpDS.broadcast(mapStageDesc);
       BroadcastConnectedStream<JSONObject, JSONObject> connect = KafkaMainStreamMap.connect(broadcastDs);
       connect.process(new ProcessSpiltStreamToHBaseDim(mapStageDesc));




       env.execute();
    }
}