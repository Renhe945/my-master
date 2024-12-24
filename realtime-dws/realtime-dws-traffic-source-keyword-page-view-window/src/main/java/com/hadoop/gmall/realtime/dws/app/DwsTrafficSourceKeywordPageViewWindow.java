package com.hadoop.gmall.realtime.dws.app;

import com.bw.gmall.realtime.common.base.BaseSqlApp;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.function.KwSplit;
import com.bw.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSqlApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW, 1, 10021);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        /**从页面日志事实表中读取数据 创建动态表 并指定Watermark的生成策略以及提取事件时间字段
         * 1. 读取 页面日志
        */
        tableEnv.executeSql("create table page_log(" +
                "common map<string,string>, "+
                " page map<string, string>,  " +
                " ts bigint, " +
                " et as to_timestamp_ltz(ts, 3), " +
                " watermark for et as et - interval '5' second " +
                ")" +
                SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));
//        tableEnv.executeSql("select * from page_log").print();
        //注册自定义函数到表执行环境中
        //从贞面日志事实表中读取数据 创建动态表 并指定Watermark的生成策略以及提取事件时间字段


        // 过滤出搜索行为
        Table kwTable = tableEnv.sqlQuery("select " +
                "page['item'] kw, " +
                "et " +
                "from page_log " +
                "where ( page['last_page_id'] ='search' " +
                "        or page['last_page_id'] ='home' " +
                "       )" +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");
        tableEnv.createTemporaryView("kw_table", kwTable);
//        kwTable.execute().print();


        //调用自定义函数充成分词
        // 3. 自定义分词函数
        tableEnv.createTemporaryFunction("kw_split", KwSplit.class);

        // 2. 读取搜索关键词
        Table keywordTable = tableEnv.sqlQuery("select " +
                " keyword, " +
                " et " +
                "from kw_table " +
                "join lateral table(kw_split(kw)) on true ");
        tableEnv.createTemporaryView("keyword_table", keywordTable);
//        keywordTable.execute().print();

        //并和原表的其它字段进行join
        //分组、开窗、粜合
        // 3. 开窗聚和
        Table result = tableEnv.sqlQuery("select " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                " date_format(window_start, '2024-11-20') cur_date, " +
                " keyword," +
                " count(*) keyword_count " +
                "from table( tumble(table keyword_table, descriptor(et), interval '5' second ) ) " +
                "group by window_start, window_end, keyword ");
//        result.execute().print();
        // 5. 写出到 doris 中
        //将察合的结果写到Doris中

        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 2023-07-11 14:14:14
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + Constant.FENODES + "'," +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                "  'username' = 'admin'," +
                "  'password' = 'zh1028,./', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        result.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}