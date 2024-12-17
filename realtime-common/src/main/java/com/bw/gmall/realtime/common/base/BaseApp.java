package com.bw.gmall.realtime.common.base;


import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {

    public abstract void handle(StreamExecutionEnvironment env,DataStreamSource<String> dataStreamSource);
    public void start(String topicDb,String groupId,int p,int port)  {
        // 1. 设置Hadoop执行用户
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);
        //1.1准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2.设置并行度
        env.setParallelism(p);
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
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //设置状态后端以及检查点 存储路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck"+groupId);
//        设置操作Hadoop的用户
        System.setProperty("HADOOP_USER_NAME","root");
//    从kafka中获取主题 topic_db 主题中读取业务数据
//        声明消费的主题和消费者组
//        String topic="topic_db";
//        String groupID="dim_app_group";
        //创建消费者对象(优化后的）
        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(topicDb, groupId);
        //将其封装为流
        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // 2. 执行具体的处理逻辑
        handle(env, kafkaSource);
        // 7.执行
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
