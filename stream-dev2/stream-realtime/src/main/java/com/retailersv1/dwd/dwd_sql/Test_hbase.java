package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
  @ClassDescription:
  @Author:fangyu_ren
  @Create:2024/12/30 S{TIME}
**/public class Test_hbase {

    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String Connector="hbase-2.2";
    private static final String Hbase_sql ="CREATE  TABLE base_dic (\n" +
            "    rowkey string,\n" +
            "    info ROW<dic_name string,parent_code string>,\n" +
            "    PRIMARY KEY (rowkey) NOT ENFORCED\n" +
            ") WITH (\n" +
            "    'connector' = 'hbase-2.2',\n" +
            "    'table-name' = 'gmall:dim_base_dic',\n" +
            "    'zookeeper.quorum' = '"+CDH_ZOOKEEPER_SERVER+"'\n" +
            ")";
    public static void main(String[] args) {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv= StreamTableEnvironment.create(env);
        String hiveConfDir = "E://git仓库//my-master//stream-dev2//stream-realtime//src//main//resources";
        HiveCatalog hiveCatalog =new HiveCatalog("hive-catalog","default",hiveConfDir);
        tEnv.registerCatalog("hive-catalog",hiveCatalog);
        tEnv.useCatalog("hive-catalog");
        tEnv.executeSql(Hbase_sql).print();
    }
}