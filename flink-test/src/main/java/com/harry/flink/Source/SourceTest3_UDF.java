package com.harry.flink.Source;

import com.harry.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class SourceTest3_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<SensorReading> source = env.addSource(new MySensorSource());
        source.print();
        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {

        //定义一个标志位
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            //定义一个随机数发生器
            Random random = new Random();

             HashMap<String,Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_"+(i+1), random.nextGaussian()*20+60 );
            }

            while (running){
                for (String sensorId : sensorTempMap.keySet()) {
                    Double newTemp = sensorTempMap.get(sensorId)+random.nextGaussian();
                    sensorTempMap.put(sensorId,newTemp);
                    sourceContext.collect(new SensorReading(sensorId, System.currentTimeMillis(),newTemp));
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
