package com.harry.flink.Source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.readTextFile("C:\\Users\\Harry\\IdeaProjects\\scala_tutor\\src\\main\\resources\\sensor.txt");
        dataStream.print();
        env.execute();
    }
}
