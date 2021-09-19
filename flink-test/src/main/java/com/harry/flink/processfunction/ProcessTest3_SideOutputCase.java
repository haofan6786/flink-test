package com.harry.flink.processfunction;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest3_SideOutputCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义OutputTag
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("lowTemp") {
        };

        //自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                        //判断温度大于30，输出到主流，小的话就输出到侧输出流
                        if (value.getTemperature()>30){
                            out.collect(value);
                        }
                        else {
                            ctx.output(outputTag,value);
                        }
                    }
                });

        highTempStream.print("high-temp");
        highTempStream.getSideOutput(outputTag).print("low-temp");


        env.execute();
    }
}
