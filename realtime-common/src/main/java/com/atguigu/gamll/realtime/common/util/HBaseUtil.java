package com.atguigu.gamll.realtime.common.util;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

//HBase的工具类
public class HBaseUtil {
//    获取Hbase的连接
//    公开静态
    public static Connection getHbaseCo() throws IOException {
        Connection hbasecon = ConnectionFactory.createConnection();//链接工厂
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return hbasecon;
    }
        //关闭Hbase的连接
    public static void closeHbasecon(Connection con) throws IOException {
        if(con!=null && con.isClosed()){//关闭连接
            con.close();
        }
    }
    //建表
    public static void selectHbasecon(Connection hbaseconn,String name,String tableName,String ... famils){
        if(famils.length<1) {//如果列族为空
            System.out.println("需要一个列族");
            return;
        }
        try (Admin admin = hbaseconn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(name, tableName);
            if(admin.tableExists(tableNameObj)) {
                System.out.println("表空间在"+name + " 下的表 " + tableName + " 已存在");
                return;
            }
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String famil : famils) {
                ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(famil)).build();
                builder.setColumnFamily(familyDescriptor);
            }
            admin.createTable(builder.build());
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
//        先获取admin
        }

        //    删除表
    public static void deleteHbasecon(Connection hbaseconn,String name,String tableName){
        try (Admin admin = hbaseconn.getAdmin()){
            TableName table = TableName.valueOf(name, tableName);
            if (admin.tableExists(table)) {
                System.out.println("要删除的表空间在"+name + " 下的表 " + table + " 不存在");
            }
            admin.disableTable(table);
            admin.deleteTable(table);
            System.out.println("删除的表空间在"+name + " 下的表 " + table + " 删除成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }





}
