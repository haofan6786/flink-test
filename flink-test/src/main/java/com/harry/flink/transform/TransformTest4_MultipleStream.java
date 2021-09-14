package com.harry.flink.transform;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest4_MultipleStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("C:\\Users\\Harry\\IdeaProjects\\scala_tutor\\src\\main\\resources\\sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line->{

        })
    }
}
