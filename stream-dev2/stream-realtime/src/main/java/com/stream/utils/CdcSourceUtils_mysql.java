package com.stream.utils;

import com.stream.common.utils.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

/**
 * @Package com.stream.common.utils.CdcSourceUtils_mysql
 * @Author zhou.han
 * @Date 2024/12/17 11:49
 * @description: MySQL Cdc Source
 * 读取sql数据
 */
public class CdcSourceUtils_mysql {

    public static MySqlSource<String> getMySQLCdcSource(String database,String table,String username,String pwd,StartupOptions model){
        return  MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .databaseList(database)
                .tableList(table)
                .username(username)
                .password(pwd)
                .serverTimeZone(ConfigUtils.getString("mysql.timezone"))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(model)
                .includeSchemaChanges(true)
                .build();



    }
}
