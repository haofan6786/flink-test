package com.harry.flink.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
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
        /**
         * 查询转换
         * Table API
         * 简单转换
         */
        Table resultTable = inputTable.select("id,temperature").where("id = 'sensor_6'");
        Table aggTable = inputTable.groupBy("id").select("id, id.count as count, temperature.avg as avgTemp");



        String outputPath = "/Users/harry/IdeaProjects/flink-test/flink-test/src/main/resources/out.txt";
        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("cnt",DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        env.execute();
    }
}
