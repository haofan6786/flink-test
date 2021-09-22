package com.harry.flink.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableTest4_KafkaPipeline {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //连接kafka，读取数据
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect", "hadoop200:2181")
                .property("bootstrap.servers", "hadoop200:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamps", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        //简单转换
        Table sensorTable = tableEnv.from("inputTable");
        Table resultTable = sensorTable.select("id,temperature").where("id = 'sensor_6'");
        Table aggTable = sensorTable.groupBy("id").select("id, id.count as count, temperature.avg as avgTemp");

        //建立Kafka连接，输出到不同的topic下
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sinktest")
                .property("zookeeper.connect", "hadoop200:2181")
                .property("bootstrap.servers", "hadoop200:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
//                        .field("timestamps", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");
        env.execute();
    }
}
