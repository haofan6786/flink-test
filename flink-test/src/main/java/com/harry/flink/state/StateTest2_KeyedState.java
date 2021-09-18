package com.harry.flink.state;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        //定义一个有状态的map操作，统计当前sensor数据的个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper());

        resultStream.print();
        env.execute();
    }

    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {

        private ValueState<Integer> keyCountState;

        //其他类型状态的声明
        /**
         *private ListState<String> myListState;
         *private MapState<String> mapState;
         *private ReducingState<SensorReading> reducingState;
         */

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Integer>(
                            "key-count",Integer.class));
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            Integer count = keyCountState.value();
            if (count==null) count = 0;
            count++;
            keyCountState.update(count);
            return count;
        }
    }
}
