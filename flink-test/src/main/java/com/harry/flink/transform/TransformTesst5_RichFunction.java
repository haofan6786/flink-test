package com.harry.flink.transform;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTesst5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("/Users/harry/IdeaProjects/flink-test/flink-test/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Tuple2<String,Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();
        env.execute();

    }

    public static class MyMapper0 implements MapFunction<SensorReading,Tuple2<String,Integer>>{
        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(),sensorReading.getId().length());
        }
    }

    public static class MyMapper extends RichMapFunction<SensorReading,Tuple2<String,Integer>>{
        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(),getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化工作，一般是定义状态，或者简历数据库连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            //一边是关闭连接和清空状态的收尾操作
            System.out.println("close");
        }
    }
}
