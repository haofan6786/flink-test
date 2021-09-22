package com.harry.flink.tableapi;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest1_Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.readTextFile("/Users/harry/IdeaProjects/flink-test/flink-test/src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],
                    new Long(fields[1]),
                    new Double(fields[2]));
        });

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        //调用table API进行转换操作
        Table resultTable = dataTable.select("id,temperature")
                .where("id='sensor_1'");

        //执行SQL
        tableEnv.createTemporaryView("sensor",dataTable);
        String sql = "select id,temperature from sensor where id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable,Row.class).print("sql");

        env.execute();
    }
}
