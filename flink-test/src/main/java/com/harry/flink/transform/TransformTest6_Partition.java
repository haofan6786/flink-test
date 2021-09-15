package com.harry.flink.transform;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("/Users/harry/IdeaProjects/flink-test/flink-test/src/main/resources/sensor.txt");

        inputStream.print("input");

        //1.shuffle
        DataStream<String> shuffleStream = inputStream.shuffle();
        shuffleStream.print("shuffle");

        //2.keyBy
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
//
//        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
//
//        keyedStream.print("keyBy");


        dataStream.global().print("global");
        env.execute();
    }
}
