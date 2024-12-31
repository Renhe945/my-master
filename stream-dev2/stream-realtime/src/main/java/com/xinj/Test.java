package com.xinj;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
  @ClassDescription:
  @Author:fangyu_ren
  @Create:2024/12/30 S{TIME}
**/
public class Test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv=StreamTableEnvironment.create(env);
        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog","default",  "E://git仓库//my-master//stream-dev2//stream-realtime//src//main//resources");//放哪
        tenv.registerCatalog("hive-catalog",hiveCatalog);
        tenv.useCatalog("hive-catalog");
        tenv.executeSql("select rowkey,info.dic_name as dic_name, info.parent_code as parent_code from base_dic").print();







    }
}