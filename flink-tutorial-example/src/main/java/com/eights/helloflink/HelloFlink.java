package com.eights.helloflink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 案例来自 孙金城 - flink课程
 * 消费kafka的数据 写入mysql
 */
public class HelloFlink {

    public static void main(String[] args) throws Exception {

        String kafkaSoruceDDL = "create table kafka_source (\n" +
                " msg string\n" +
                ") with (\n" +
                " 'connector' = 'kafka-0.11', \n" +
                " 'topic' = 'cdh-log',\n" +
                " 'properties.bootstrap.servers' = '192.168.127.121:9092',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset'\n" +
                ")";

        String mysqlSinkDDL = "CREATE TABLE mysql_sink (\n" +
                " msg STRING \n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://192.168.127.121:3306/flinkdb?characterEncoding=utf-8&useSSL=false',\n" +
                "   'table-name' = 'cdn_log',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456',\n" +
                "   'sink.buffer-flush.max-rows' = '1'\n" +
                ")";

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql(kafkaSoruceDDL);
        tEnv.executeSql(mysqlSinkDDL);

        Table source = tEnv.from("kafka_source");

        source.executeInsert("mysql_sink");

        tEnv.execute("Flink Hello World");
    }



}
