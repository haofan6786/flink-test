package com.harry.flink.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1基于老版本planner的流处理
        EnvironmentSettings oldStreamSettings
                = EnvironmentSettings
                .newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv
                = StreamTableEnvironment
                .create(env, oldStreamSettings);

        //1.2基于老版本planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        //1.3 blink stream
        EnvironmentSettings blinkStreamSettings
                = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv
                = StreamTableEnvironment
                .create(env, blinkStreamSettings);

        //基于blink的批处理
        EnvironmentSettings blinkBatchSettings
                = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnv
                = TableEnvironment
                .create(blinkBatchSettings);

        /**
         * 2.表的创建：连接外部系统，读取数据
         * 2.1 读取文件
         */
        String filePath = "/Users/harry/IdeaProjects/flink-test/flink-test/src/main/resources/sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamps", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();
        /**
         * 查询转换
         * Table API
         * 简单转换
         */
        Table resultTable = inputTable.select("id,temperature").where("id = 'sensor_6'");
        Table aggTable = inputTable.groupBy("id").select("id, id.count as count, temperature.avg as avgTemp");



        //3.2 SQL
        Table sqlQuery = tableEnv.sqlQuery("select id,temperature from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id,count(id)as cnt,avg(temperature) as avgTemp from inputTable group by id");

        tableEnv.toAppendStream(resultTable,Row.class).print("result");
        tableEnv.toRetractStream(aggTable,Row.class).print("agg");
        tableEnv.toRetractStream(sqlAggTable,Row.class).print("sqlAgg");


        env.execute();
    }
}
