package com.harry.flink.tableapi;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //2.读入文件数据，得到DataStream
        DataStreamSource<String> inputStream = env.readTextFile("/Users/harry/IdeaProjects/flink-test/flink-test/src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],
                    new Long(fields[1]),
                    new Double(fields[2]));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                });


        //4 将流转化成表，定义时间特性
//        Table dataTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature as temp,pt.proctime");
        Table dataTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature as temp,rt.rowtime");


        tableEnv.createTemporaryView("sensor",dataTable);

        /**
         * 窗口操作
         * 5.1 Group Window
         * table API
         */
        Table resultTable = dataTable.window(Tumble.over("10.seconds")
                .on("rt")
                .as("tw")
        )
                .groupBy("id,tw")
                .select("id,id.count,temp.avg,tw.end");

        //SQL
        Table resultSqlTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temp) as avgTemp, tumble_end(rt,interval '10' second) " +
                "from sensor group by id ,tumble(rt,interval '10' second)");


        /**
         * Over Window
         * table API
         */
        Table overResult = dataTable.window(Over.partitionBy("id")
                .orderBy("rt")
                .preceding("2.rows")
                .as("ow")).select("id,rt,id.count over ow,temp.avg over ow");


        Table overSqlResult = tableEnv.sqlQuery("select id, rt, count(id) over ow,avg(temp) over ow" +
                " from sensor " +
                " window ow as (partition by id order by rt rows between 2 preceding and current row)");


//        tableEnv.toAppendStream(resultTable, Row.class).print();
//        tableEnv.toRetractStream(resultSqlTable,Row.class).print("result");

        tableEnv.toAppendStream(overResult, Row.class).print("result");
        tableEnv.toRetractStream(overSqlResult,Row.class).print("sql");

        env.execute();
    }
}
