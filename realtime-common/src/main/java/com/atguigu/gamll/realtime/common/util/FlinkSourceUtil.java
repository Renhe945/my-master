package com.atguigu.gamll.realtime.common.util;

import com.atguigu.gamll.realtime.common.constant.Constant;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FlinkSourceUtil {
<<<<<<< HEAD:gmall-2023-realtime/realtime-common/src/main/java/util/FlinkSourceUtil.java
    public static KafkaSource<String> getKafkaSource(String groupId,
                                                     String topic) {
=======
    public static KafkaSource<String> getKafkaSource(String groupId, String topic){

>>>>>>> efad88b78fb06f1e97470a9e3261a1002227c3c3:realtime-common/src/main/java/com/atguigu/gamll/realtime/common/util/FlinkSourceUtil.java
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message, StandardCharsets.UTF_8);
                        }
                        return null;
                    }
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                .build();
    }
<<<<<<< HEAD:gmall-2023-realtime/realtime-common/src/main/java/util/FlinkSourceUtil.java
}
=======
}

>>>>>>> efad88b78fb06f1e97470a9e3261a1002227c3c3:realtime-common/src/main/java/com/atguigu/gamll/realtime/common/util/FlinkSourceUtil.java
