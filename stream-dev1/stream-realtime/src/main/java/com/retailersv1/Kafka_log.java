package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Properties;

/**
  @ClassDescription:
  @Author:fangyu_ren
  @Create:2024/12/24 S{TIME}
 读取kafka 日志数据
**/


public class Kafka_log {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> build = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
                .setTopics("topic_log")
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
        DataStreamSource<String> kafka_source = env.fromSource(build, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafka_source.print("读取  kafka 日志数据");
        //进行数据的转换将无法解析成JSON的过滤掉
        OutputTag<String> outputTag = new OutputTag<String>("outputTag") {};
        SingleOutputStreamOperator<JSONObject> json = kafka_source.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject json = JSONObject.parseObject(s);//这是可以解析的JSON传到下游
                    out.collect(json);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        json.print("正常数据");
        SideOutputDataStream<String> sideOutput_zsj = json.getSideOutput(outputTag);//测流
        sideOutput_zsj.print("脏数据——————————");
        //将侧输出流中的脏数据写到kafka主题中
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("sideOutput_zsj");
        sideOutput_zsj.sinkTo(kafkaSink);
        //TODO 对新老访客标记进行修复
        KeyedStream<JSONObject, String> mid_key = json.keyBy(j -> j.getJSONObject("common").getString("mid"));//根据用户ID进行分组





    }
}