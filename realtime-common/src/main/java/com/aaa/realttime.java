package com.aaa;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class realttime {
    @SneakyThrows
    public static void main(String[] args) {
//        StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream(  "cdh01", 9999);
        streamSource.print();
        env.execute();
    }
}
