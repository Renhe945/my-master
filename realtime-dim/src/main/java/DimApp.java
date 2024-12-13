import com.atguigu.gamll.realtime.common.bean.TableProcessDim;
import com.atguigu.gamll.realtime.common.constant.Constant;
//import com.atguigu.gamll.realtime.common.constant.JsonDebeziumDeserializationUtil1;
import com.atguigu.gamll.realtime.common.constant.JsonDebeziumDeserializationUtil1;
import com.atguigu.gamll.realtime.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Properties;

public class DimApp {//extends BaseApp
    public static void main(String[] args) throws Exception {
        //1.1准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2.设置并行度
        env.setParallelism(4);
        //2.检查点相关的设置
        //2.1开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);//读的最少一次
        //2.2.设置 检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3.设置jod取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4设置两个点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(3),Time.seconds(3)));
        //设置状态后端以及检查点 存储路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        设置操作Hadoop的用户
        System.setProperty("HADOOP_USER_NAME","root");
//    从kafka中获取主题 topic_db 主题中读取业务数据
//        声明消费的主题和消费者组
        String topic="topic_db";
        String groupID="dim_app_group";
//        创建消费者对象
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics(Constant.TOPIC_DB)//Constant.
                .setGroupId(groupID)
                //从最末尾未开始消费
                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())//自带的会在String类型的时候反序列化然后报错
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if(message!=null){
                            return new String(message);
                        }
                        return null;
                    }
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }
                    @Override
                    public TypeInformation<String> getProducedType() {
//                        当前为字符串
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        //将其封装为流
        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        //将数据进行转换
        SingleOutputStreamOperator<JSONObject> json_objds = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject json = JSON.parseObject(s);
//                拆分数据
                String db = json.getString("database");
                String type = json.getString("type");
                String data = json.getString("data");
                if ("gmall".equals(db) && ("insert".equals(type)) || "update".equals(type) || "delete".equals(type) || "bootstrap-insert".equals(type) && data != null && data.length() > 2) {
                    collector.collect(json);
                }
            }
        });
//        json_objds.print();
//        使用FlinkCDC读取配置表中的配置信息
//        创建mysqlsource对象
        Properties prop = new Properties();
        prop.setProperty("useSSL","false");
        prop.setProperty("allowPublickeyRetrieval","true");
        MySqlSource<String> mysql_build = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall_config") // monitor all tables under inventory database
                .tableList("gmall_config.table_process_dim")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationUtil1()) // converts SourceRecord to String
//                .startupOptions(StartupOptions.earliest())// 从最开始查询
                .startupOptions(StartupOptions.initial())// 从最开始查询
                .jdbcProperties(prop)
                .build();
            //读取数据封装为流
//        env.addSource(build).print().setParallelism(1);
        DataStreamSource<String> mysql_source = env.fromSource(mysql_build, WatermarkStrategy.noWatermarks(), "mysql_Source").setParallelism(1);
//        mysql_source.print();//读取表
//      r 添加
//      d删除
//      对配置流中的数据类型进行转换
        SingleOutputStreamOperator<TableProcessDim> toos = mysql_source.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String s) throws Exception {
//                为了处理方便将s转换为jsonOBJ
                JSONObject json = JSON.parseObject(s);
                String op = json.getString("op");
                TableProcessDim tableProcessDim = null;
                if (op.equals("d")) {//进行删除操作
                    tableProcessDim = json.getObject("data", TableProcessDim.class);
                } else {//对配置进行添加修改读取
                    tableProcessDim = json.getObject("data", TableProcessDim.class);
                }
                tableProcessDim.setOp(op);
                return tableProcessDim;
            }
        }).setParallelism(1);
//        toos.print();
        SingleOutputStreamOperator<TableProcessDim> map_Hbase = toos.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
            private Connection hbaseConn;
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HBaseUtil.getHbaseCo();
                super.open(parameters);
            }
            @Override
            public void close() throws Exception {
                HBaseUtil.closeHbasecon(hbaseConn);
            }
            @Override
            public TableProcessDim map(TableProcessDim ta) throws Exception {
//                获取配置表进行的操作类型
                String op = ta.getOp();
                String table = ta.getSinkTable();
//                获取HBASE中维度标的表明
                String[] sinkFa = ta.getSinkFamily().split(",");
                if ("d".equals(op)) {
//                    在配置表中删除一个条数据在HBASE中也删除
                    HBaseUtil.deleteHbasecon(hbaseConn, Constant.HBASE_NAMESPACE, table);
                } else if ("r".equals(op) || "c".equals(op)) {

                    HBaseUtil.selectHbasecon(hbaseConn, Constant.HBASE_NAMESPACE, table, sinkFa);
                } else { // u进行修改 应该先删除表,再建表. 表的历史数据需要重新同步
                    HBaseUtil.deleteHbasecon(hbaseConn, Constant.HBASE_NAMESPACE, table);
                    HBaseUtil.selectHbasecon(hbaseConn, Constant.HBASE_NAMESPACE, table, sinkFa);
                }
                return ta;
            }
        }).setParallelism(1);
//     8.(mysql的配置表)配置流中配置信息进行广播
        MapStateDescriptor<String, TableProcessDim> pz_gb = new MapStateDescriptor<String, TableProcessDim>("pz_gb", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = toos.broadcast(pz_gb);
//       将主流业务数据和广播流配置信息进行关联
        BroadcastConnectedStream<JSONObject, TableProcessDim> connect = json_objds.connect(broadcastDS);
//        处理关联后的数据（判断是否是维度表）
        connect.process(new BroadcastProcessFunction<JSONObject, TableProcessDim, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
//                处理主流业务数据 根据维度表名到广播流状态读取配置信息，判断是否是维度表
            }

            @Override
            public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//              处理广播留配置信息 将配置数据放到广播状态中 K维度表名  V一个配置对象
//                先获取  对配置表进行的操作的类型
                String op = tableProcessDim.getOp();
//                获取广播状态
                BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(pz_gb);
//                获取维度表名称
                String sourceTable = tableProcessDim.getSourceTable();
                if("d".equals(op)){
//              从配置表中删除了一条数据，将对应的配置信息也从广播装态中删除
                    broadcastState.remove(sourceTable);
                }
                else {
//                    对配置表进行了读取、添加或者更新操作，将最新的配置信息放到广播状态中
                    broadcastState.remove(sourceTable);
                }

            }
        });


        env.execute();
    }
}
