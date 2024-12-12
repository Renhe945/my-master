package com.atguigu.gmall.realtime.dim.app;


import base.BaseApp;import constant.Constant;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


//import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001,
                4,
                "dim_app",
                Constant.TOPIC_DB
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 对消费的数据, 做数据清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
    }
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DimApp.class);
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            String db = jsonObj.getString("database");
                            String type = jsonObj.getString("type");
                            String data = jsonObj.getString("data");

                            return "gmall".equals(db)
                                    && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                                    && data != null
                                    && data.length() > 2;

                        } catch (Exception e) {
                            log.warn("不是正确的 json 格式的数据: " + value);
                            return false;
                        }

                    }
                })
                .map(JSON::parseObject);
    }
}
