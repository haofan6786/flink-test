package com.harry.flink.window;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Double> avgTempResultStream = dataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {

                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0.0, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> accumulator) {
                        return new Tuple2<>(accumulator.f0 + sensorReading.getTemperature(), accumulator.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> acc1) {
                        return new Tuple2<>(acc1.f0 + doubleIntegerTuple2.f0, acc1.f1 + doubleIntegerTuple2.f1);
                    }
                });

        /**
         * 其他可选API
         * 1.trigger()
         * 2.evictor()
         * 3.allowedLateness(Time.seconds(15))
         * 4.sideOutputLateData( new OutputTag<>(){} )
         */

        avgTempResultStream.print();
        env.execute();
    }
}
