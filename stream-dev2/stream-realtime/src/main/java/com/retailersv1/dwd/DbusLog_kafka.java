package com.retailersv1.dwd;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.utils.CdcSourceUtils_mysql;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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
        MySqlSource<String> mysqlSource = CdcSourceUtils_mysql.getMySQLCdcSource(//定义了一个mysql的读取
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
//        KafkaSource<String> build = KafkaSource.<String>builder()//读取kafka的数据
//                .setBootstrapServers(ConfigUtils.getString("kafka.bootstrap.servers"))//cdh01
//                .setTopics(ConfigUtils.getString("kafka.topic.db"))//topic_db
//                .setGroupId("groupId")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new DeserializationSchema<String>() {//反序列化逻辑，
//                    @Override
//                    public String deserialize(byte[] bytes) throws IOException {
//                        if (bytes != null) {  return new String(bytes, StandardCharsets.UTF_8);}
//                        return null;
//                    }
//                    @Override
//                    public boolean isEndOfStream(String s) {
//                        return false;
//                    }
//                    @Override
//                    public TypeInformation<String> getProducedType() {
//                        return Types.STRING;
//                    }
//                }).build();
        DataStreamSource<String> kafka_source = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "kafka_Source");
        kafka_source.print("读取 业务数据");

        OutputTag<String> kafka_db = new OutputTag<String>("kafka_db"){};
        SingleOutputStreamOperator<JSONObject> processDS = kafka_source.process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                        try {
                            collector.collect(JSONObject.parseObject(s));
                        } catch (Exception e) {
                            context.output(kafka_db, s);
                            System.err.println("错误的数据!");
                        }
                    }
                }).uid("convert_json_process")
                .name("convert_json_process");
        processDS.print("转换过的");

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
//配置表数据->{"before":null,"after":{"source_table":"base_dic","sink_table":"dim_base_dic","sink_family":"info","sink_columns":"dic_code,dic_name","sink_row_key":"dic_code"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2023_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1735181463934,"transaction":null}
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
//
       SingleOutputStreamOperator<JSONObject> tpDS = StreamMapCleanColumn.map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER, CDH_HBASE_NAME_SPACE))
               .uid("map_create_hbase_dim_table")
               .name("map_create_hbase_dim_table");
       MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
       BroadcastStream<JSONObject> broadcastDs = tpDS.broadcast(mapStageDesc);


       BroadcastConnectedStream<JSONObject, JSONObject> connect = KafkaMainStreamMap.connect(broadcastDs);
       // 3.主流跟配置进行连接，根据配置过滤要的流
       SingleOutputStreamOperator<JSONObject> zl_pl = connect.process(new ProcessKafka(mapStageDesc));
        zl_pl.print("发送到kafka");
//      发送到kafka
//        发给撒饭


       env.execute();
    }




}
