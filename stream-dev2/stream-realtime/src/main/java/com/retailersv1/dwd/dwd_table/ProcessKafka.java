package com.retailersv1.dwd;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDim;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
  @ClassDescription:
  @Author:fangyu_ren
  @Create:2024/12/26 S{TIME}
**/public class ProcessKafka extends BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>{

        private MapStateDescriptor<String,JSONObject> mapStateDescriptor;
        private HashMap<String, TableProcessDim> configMap =  new HashMap<>();

    public ProcessKafka(MapStateDescriptor<String, JSONObject> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
        public void open(Configuration parameters) throws Exception {
        // 手动读取配置表
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        String querySQL = "select * from gmall2023_config.table_process_dim";//读取配置表
        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);
        // configMap:spu_info -> TableProcessDim(sourceTable=spu_info, sinkTable=dim_spu_info, sinkColumns=id,spu_name,description,category3_id,tm_id, sinkFamily=info, sinkRowKey=id, op=null)
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }

    }
        @Override
        public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
            //主流
        ReadOnlyBroadcastState<String, JSONObject> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            String tableName = jsonObject.getJSONObject("source").getString("table");
            JSONObject broadData = broadcastState.get(tableName);
            // 这里可能为null NullPointerException
            if (broadData != null || configMap.get(tableName) != null){
                //筛选出可以关联的数据
                if (configMap.get(tableName).getSourceTable().equals(tableName)){
                    System.err.println(jsonObject);
                    if(!"d".equals(jsonObject.getString("op"))){
                        //获取关联后的发送主题名称
                        String sinkTable = configMap.get(tableName).getSinkTable();
                        JSONObject kafkaData = new JSONObject();
                        kafkaData.put("tableName",tableName);//将主流的表明添加进去
                        kafkaData.put("operation",jsonObject.getString("op"));
                        kafkaData.put("data",jsonObject.getJSONObject("after"));
                        System.out.println(">>>>>>>"+kafkaData);
//>>>>>>>{"data":{"birthday":10845,"create_time":1731312072000,"login_name":"5re6bjer","nick_name":"阿维","name":"姜维","user_level":"2","phone_num":"13344756831","id":53,"email":"5re6bjer@googlemail.com"},"operation":"r","tableName":"user_info"}
                        //collector.collect(kafkaData);
                        ArrayList<JSONObject> list = new ArrayList<>();
                        list.add(kafkaData);
                        for (JSONObject object : list) {
                            System.out.println(sinkTable+":"+object.toString());
                        }
                        KafkaUtils.sinkJson2KafkaMessage(sinkTable,list);
                    }
                }
            }
        }
//{"op":"r","after":{"birthday":1591,"create_time":1731288310000,"login_name":"43obxqzrj","nick_name":"阿炎","name":"伏炎","user_level":"1","phone_num":"13816784411","id":262,"email":"43obxqzrj@sina.com"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall","table":"user_info"},"ts_ms":1735216419838}
        @Override
        public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//            读取mysql的表生成配置流
        // {"op":"r","after":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_base_category2","source_table":"base_category2","sink_columns":"id,name,category1_id"}}
            BroadcastState<String, JSONObject> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            // HeapBroadcastState{stateMetaInfo=RegisteredBroadcastBackendStateMetaInfo{name='mapStageDesc', keySerializer=org.apache.flink.api.common.typeutils.base.StringSerializer@39529185, valueSerializer=org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer@59b0797e, assignmentMode=BROADCAST}, backingMap={}, internalMapCopySerializer=org.apache.flink.api.common.typeutils.base.MapSerializer@4ab01899}
            String op = jsonObject.getString("op");
            if (jsonObject.containsKey("after")){
                String sourceTableName = jsonObject.getJSONObject("after").getString("source_table");
                if ("d".equals(op)){
                    broadcastState.remove(sourceTableName);//删除
                }else {
                    broadcastState.put(sourceTableName,jsonObject);
                }
            }
        }
    }

