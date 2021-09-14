package com.harry.flink.transform;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class TransformTest4_MultipleStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("C:\\Users\\Harry\\Documents\\GitHub\\flink-test\\flink-test\\src\\main\\resources\\sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SplitStream<SensorReading> splitStream = dataStream.split((OutputSelector<SensorReading>) sensorReading -> {
            return (sensorReading.getTemperature() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
        });

        DataStream<SensorReading> high = splitStream.select("high");
        DataStream<SensorReading> low = splitStream.select("low");
        DataStream<SensorReading> all = splitStream.select("high", "low");
        high.print("high");
        low.print("low");
        all.print("all");

        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream = high.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(),sensorReading.getTemperature());
            }
        });
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(low);

        SingleOutputStreamOperator<Object> result = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

        result.print();

        env.execute();
    }
}
