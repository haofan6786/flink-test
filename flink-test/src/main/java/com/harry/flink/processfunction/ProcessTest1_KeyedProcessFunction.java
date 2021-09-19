package com.harry.flink.processfunction;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessTest1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //先分组，然哦吼自定义处理
        SingleOutputStreamOperator<Integer> resultStream = dataStream.keyBy("id")
                .process(new KeyedProcessFunction<Tuple, SensorReading, Integer>() {

                    ValueState<Long> tsTimerState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        tsTimerState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<Long>(
                                        "ts-timer",Long.class));
                    }

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
                        out.collect(value.getId().length());

                        //context
                        ctx.timestamp();
                        ctx.getCurrentKey();
                        //ctx.output();
                        ctx.timerService().currentProcessingTime();
                        ctx.timerService().currentWatermark();
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService()
                                .currentProcessingTime() + 1000L);
                        tsTimerState.update((ctx.timerService()
                                .currentProcessingTime() + 1000L));
//                        ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000L);
//                        ctx.timerService().deleteEventTimeTimer(10000L);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
                        System.out.println(timestamp + "定时器触发");
                        ctx.getCurrentKey();
//                        ctx.output();
                        ctx.timeDomain();
                    }

                    @Override
                    public void close() throws Exception {
                        tsTimerState.clear();
                    }
                });

        env.execute();
    }
}
