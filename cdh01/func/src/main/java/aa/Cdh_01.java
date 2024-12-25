package aa;



import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static a2.Flinkmysql_KafkaProducer1.*;

//./bin/flink run-application -t yarn-application -c aa.Cdh_01 /data/original-common-1.0-SNAPSHOT.jar
//./bin/flink run-application -t yarn-application -c aa.Cdh_01 /data/common-1.0-SNAPSHOT.jar
public class Cdh_01 {
    public static void main(String[] args) throws Exception {
    //flink cdc mysql ->sink to kafka
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //        设置并行度
        env.setParallelism(1);
            MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                    .hostname("cdh03")
                    .port(3306)
                    .databaseList("test") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                    .tableList("test.aa") // 设置捕获的表
                    .username("root")
                    .password("root")
                    .startupOptions(StartupOptions.initial())
                    .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                    .build();
            // 设置 3s 的 checkpoint 间隔
            env.enableCheckpointing(3000);
            DataStreamSource<String> data = env.fromSource(
                    mySqlSource,
            WatermarkStrategy.noWatermarks(),
                    "MySQL Source");
            data.print();
        env.execute();
    }
}
//
//        // 配置 Kafka Sink将MySQL的数据发送到 Kafka
//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers(Servers)
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic(Topic)
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build()
//                ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .build();
//
//        data.sinkTo(sink);



