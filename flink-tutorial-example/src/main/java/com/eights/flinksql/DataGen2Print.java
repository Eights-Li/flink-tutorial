package com.eights.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 来自孙金城 flink知其然知其所以然课程
 * 查看flink sql的执行计划和codegen
 */
public class DataGen2Print {

    public static void main(String[] args) throws Exception {
        // DataGen
        String sourceDDL = "CREATE TABLE stu (\n" +
                " name STRING , \n" +
                " age INT \n" +
                ") WITH (\n" +
                " 'connector' = 'datagen' ,\n" +
                " 'fields.name.length'='10'\n" +
                ")";

        // Print
        String sinkDDL = "CREATE TABLE rst (\n" +
                " name STRING , \n" +
                " age INT , \n" +
                " weight INT  \n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")";


        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, settings);

        //注册source和sink
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        String sql = "INSERT INTO rst SELECT stu.name, stu.age-2, 35+2 FROM stu";

        String plan = tEnv.explainSql(sql);
        System.out.println(plan);

        tEnv.executeSql(sql);
    }
}
