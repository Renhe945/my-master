import com.atguigu.gamll.realtime.common.constant.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

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
        json_objds.print();

        env.execute();
    }
}
