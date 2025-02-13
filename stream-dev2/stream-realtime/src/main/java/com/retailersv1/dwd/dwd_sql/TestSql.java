package com.retailersv1.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
  @ClassDescription:
  @Author:fangyu_ren
  @Create:2024/12/30 S{TIME}
**/

import org.apache.log4j.Logger;

public class TestSql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        String createHivecatalogDDL = "create catalog hive_catalog with (\n" +
                "   'type'='hive',\n" +
                "   'default-database'='default',\n" +
                "   'hive-conf-dir'='E://git仓库//my-master//stream-dev2//stream-realtime//src//main//resources')";
        HiveCatalog hiveatalog = new HiveCatalog( "hive-catalog", "default",  "E://git仓库//my-master//stream-dev2//stream-realtime//src//main//resources");
        tenv.registerCatalog("hive-catalog",hiveatalog);
        tenv.useCatalog("hive-catalog");
        tenv.executeSql(createHivecatalogDDL).print();


    }
}