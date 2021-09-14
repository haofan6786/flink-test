package com.harry.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.readTextFile("C:\\Users\\Harry\\IdeaProjects\\scala_tutor\\src\\main\\resources\\sensor.txt");

        //1.map return the length;
        DataStream<Integer> mapStream = inputStream.map((MapFunction<String, Integer>) String::length);

        //2.flatmap split by ','
        DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) {
                String[] fields = s.split(",");
                for (String field : fields) {
                    collector.collect(field);
                }
            }
        });

        //3.filter,choose all begin with "sensor_1"
        DataStream<String> filterStream = inputStream.filter((FilterFunction<String>)s-> s.startsWith("sensor_1"));

        mapStream.print("map");
        flatMapStream.print("flatmap");
        filterStream.print("filter");

        env.execute();

    }
}
