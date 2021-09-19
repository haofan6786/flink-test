package com.harry.flink.processfunction;

import com.harry.flink.beans.SensorReading;
import lombok.AllArgsConstructor;
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

public class ProcessTest2_ApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<String> resultStream = dataStream.keyBy("id").process(new TempConsIncreWarning(10));
        resultStream.print();
        env.execute();
    }


    public static class TempConsIncreWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {

        private Integer interval;

        public TempConsIncreWarning(Integer interval){
            this.interval = interval;
        }

        //定义状态,博阿村上一次的温度
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Double>(
                            "last-temp", Double.class, Double.MIN_VALUE));
            timerTsState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Long>(
                            "timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            //如果温度上升且没有定时器的时候，开始等待，注册定时器
            if (value.getTemperature() > lastTemp && timerTs == null) {
                Long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }

            //如果温度下降，那么删除定时器
            else if (value.getTemperature() <= lastTemp && timerTs != null) {
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }

            //更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，输出报警信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0)
                    + "温度连续" + interval + "s上升");
            timerTsState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
