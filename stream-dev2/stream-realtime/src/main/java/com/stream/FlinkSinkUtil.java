package com.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
  @ClassDescription:
  @Author:fangyu_ren
  @Create:2024/12/25 S{TIME}
**/
public class FlinkSinkUtil {
    //获取KafkaSink
    public static KafkaSink<String> getKafkaSink(String topic){
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092,cdh02:9092，cdh03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //设置事务Id的前缀
                //.setTransactionalIdPrefix
                //设置事务的超时时间     检查点超时时间<    事务的超时时间 <=事务最大超时时间
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
                .build();
        return kafkaSink;
    }
}